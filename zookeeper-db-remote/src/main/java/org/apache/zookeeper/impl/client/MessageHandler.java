package org.apache.zookeeper.impl.client;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Netty connection handler that is able to send messages synchronously.
 */
public class MessageHandler extends SimpleChannelUpstreamHandler {

	private static Logger logger = LoggerFactory.getLogger(MessageHandler.class.getName());
	private ChannelHandlerContext ctx;
	private BlockingQueue<ResponseFuture<HttpResponse>> responseFutures;

	@Override
	public void channelConnected(
			ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		super.channelConnected(ctx, e);
		this.ctx = ctx;
		this.responseFutures = new LinkedBlockingQueue<>();
	}

	@Override
	public void channelDisconnected(
			ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {

		super.channelDisconnected(ctx, e);
		// All ongoing requests fail
		synchronized (this) {
			ResponseFuture<HttpResponse> prom;
			Exception err = new IOException("Connection lost");
			while ((prom = responseFutures.poll()) != null) {
				prom.setFailure(err);
			}
			responseFutures = null;
		}
	}

	public ResponseFuture<HttpResponse> sendMessage(HttpRequest httpRequest) {
		if (ctx == null)
			throw new IllegalStateException();

		ResponseFuture<HttpResponse> responseFuture = new ResponseFuture<>();

		synchronized (this) {
			if (responseFutures == null) {
				// Connection closed
				responseFuture.setFailure(new IllegalStateException());
			} else if (responseFutures.offer(responseFuture)) {
				// Connection open and message accepted
				ChannelFuture channelFuture = ctx.getChannel()
						.write(httpRequest)
						.awaitUninterruptibly();
				// Failed to send the message
				if (!channelFuture.isSuccess()) {
					responseFuture.setFailure(channelFuture.getCause());
				}
				logger.debug("Request: " + httpRequest.getUri());
			} else {
				// Connection open and message rejected
				responseFuture.setFailure(new BufferOverflowException());
			}
			return responseFuture;
		}
	}

	@Override
	public void messageReceived(
			ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		if (responseFutures != null) {
			ResponseFuture<HttpResponse> rf = responseFutures.poll();
			if (rf != null) {
				if (e.getMessage() instanceof HttpResponse) {
					if (logger.isDebugEnabled()) {
						logger.debug("Response: \r\n" + ((HttpResponse) e.getMessage()).getContent().toString(StandardCharsets.UTF_8));
					}
					rf.setResponse((HttpResponse) e.getMessage());
				}
			} else {
				logger.error("Unsupported message " + e.getMessage().getClass());
			}
		}
	}


	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
		super.exceptionCaught(ctx, e);
		if (responseFutures != null) {
			ResponseFuture<HttpResponse> rf = responseFutures.poll();
			if (rf != null) {
				logger.error("Failed to process the request", e.getCause());
				rf.setFailure(e.getCause());
			}
		}
	}
}
