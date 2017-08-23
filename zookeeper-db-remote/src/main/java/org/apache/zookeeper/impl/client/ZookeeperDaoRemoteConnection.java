package org.apache.zookeeper.impl.client;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.zookeeper.impl.node.dao.RecordNotFoundException;
import org.jboss.netty.bootstrap.ClientBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBufferInputStream;
import org.jboss.netty.buffer.ChannelBufferOutputStream;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioClientSocketChannelFactory;
import org.jboss.netty.handler.codec.http.*;
import org.jboss.netty.handler.codec.http.HttpMethod;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Keep alive connection implemented based on netty
 */
public class ZookeeperDaoRemoteConnection {

	private static volatile ZookeeperDaoRemoteConnection instance;
	private InetSocketAddress address;
	private ClientBootstrap keepAliveClient;
	private Channel channel;
	private MessageHandler clientHandler;
	private ObjectMapper objectMapper;
	private int readTimeoutMs;

	private ZookeeperDaoRemoteConnection(String host, int port) {

		this.address = new InetSocketAddress(host, port);

		objectMapper = new ObjectMapper();
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

		keepAliveClient = new ClientBootstrap(
				new NioClientSocketChannelFactory(
						Executors.newCachedThreadPool(),
						Executors.newCachedThreadPool()));

		keepAliveClient.setPipelineFactory(new ClientPipelineFactory());

		readTimeoutMs = 30000;
	}

	public static ZookeeperDaoRemoteConnection getConnection() {
		if (instance == null) {
			synchronized (ZookeeperDaoRemoteConnection.class) {
				if (instance == null) {
					String host = System.getProperty("ZOOKEEPER_HOST");
					String port = System.getProperty("ZOOKEEPER_PORT");

					if (host == null && host.isEmpty()) {
						throw new RuntimeException("Zookeeper server host is unknown");
					}

					if (port == null && port.isEmpty()) {
						throw new RuntimeException("Zookeeper server port is unknown");
					}
					instance = new ZookeeperDaoRemoteConnection(host, Integer.parseInt(port));
				}
			}
		}
		return instance;
	}

	/**
	 * Executes the HTTP request, writing the given request entity to the request, and returns the response of the requested response type.
	 *
	 * @param httpMethod
	 * @param api
	 * @param requestEntity
	 * @param responseType
	 * @param <T>
	 * @return
	 * @throws IOException
	 * @throws RecordNotFoundException
	 */
	protected <T> T sendMessage(HttpMethod httpMethod, String api, Object requestEntity, Class<T> responseType) throws IOException, RecordNotFoundException {

		MessageHandler messageHandler = getMessageHandler();

		HttpRequest request = new DefaultHttpRequest(HttpVersion.HTTP_1_1, httpMethod, api);

		int contentLength = 0;
		// set requestEntity as request content
		if (requestEntity != null) {
			ChannelBuffer content = ChannelBuffers.dynamicBuffer();
			OutputStream out = new ChannelBufferOutputStream(content);
			objectMapper.writeValue(out, requestEntity);
			request.setContent(content);
			contentLength = content.readableBytes();
		}

		request.headers().add(HttpHeaders.Names.CONTENT_LENGTH, contentLength);
		HttpHeaders.setKeepAlive(request, true);

		ResponseFuture<HttpResponse> responseFuture = messageHandler.sendMessage(request);

		try {
			// wait for response
			HttpResponse response = responseFuture.get(readTimeoutMs, TimeUnit.MILLISECONDS);
			int statusCode = response.getStatus().getCode();

			if (statusCode == 200) {
				if (responseType != null) {
					InputStream is = new ChannelBufferInputStream(response.getContent());
					return objectMapper.readValue(is, responseType);
				}
				// read the error message
			} else {
				InputStream is = new ChannelBufferInputStream(response.getContent());
				HttpError error = objectMapper.readValue(is, HttpError.class);
				if (error.getCode() == ErrorType.NODE_NOT_EXISTS.getCode()) {
					throw new RecordNotFoundException(error.getMessage());
				}
				throw new IOException(error.getMessage());
			}
		} catch (InterruptedException | TimeoutException e) {
			throw new IOException(e);
		} catch (ExecutionException e) {
			Throwable causeException = e.getCause();
			if (causeException instanceof IOException) {
				throw (IOException) causeException;
			}
			throw new IOException(causeException);
		}

		return null;
	}

	/**
	 * Return MessageHandler of the open channel.
	 *
	 * @return
	 */
	private MessageHandler getMessageHandler() {
		if (channel == null || !channel.isConnected()) {
			synchronized (this) {
				if (channel == null || !channel.isConnected()) {
					ChannelFuture future = keepAliveClient.connect(address).awaitUninterruptibly();
					if (future.isSuccess()) {
						channel = future.awaitUninterruptibly().getChannel();
						clientHandler = (MessageHandler) channel.getPipeline().getLast();
					}
				}
			}
		}

		return clientHandler;
	}

	private enum ErrorType {
		NODE_NOT_EXISTS(11);

		private int code;

		ErrorType(int code) {
			this.code = code;
		}

		public int getCode() {
			return code;
		}
	}

	private static class ClientPipelineFactory implements ChannelPipelineFactory {
		@Override
		public ChannelPipeline getPipeline() throws Exception {
			ChannelPipeline pipeline = Channels.pipeline();
			pipeline.addLast("codec", new HttpClientCodec());
			pipeline.addLast("handler", new MessageHandler());
			return pipeline;
		}
	}
}
