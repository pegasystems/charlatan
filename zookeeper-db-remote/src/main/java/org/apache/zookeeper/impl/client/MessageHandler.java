package org.apache.zookeeper.impl.client;

import org.jboss.netty.channel.*;
import org.jboss.netty.handler.codec.http.HttpChunk;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponse;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Netty connection handler that is able to send messages synchronously.
 */
public class MessageHandler extends SimpleChannelUpstreamHandler {

	private ChannelHandlerContext ctx;
	private BlockingQueue<ResultFuture<HttpResponse>> messageList = new LinkedBlockingQueue<>();

	@Override
	public void channelConnected(
			ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {
		super.channelConnected(ctx, e);
		this.ctx = ctx;
	}

	@Override
	public void channelDisconnected(
			ChannelHandlerContext ctx, ChannelStateEvent e) throws Exception {

		super.channelDisconnected(ctx, e);
		synchronized (this) {
			ResultFuture<HttpResponse> prom;
			Exception err = new IOException("Connection lost");
			while ((prom = messageList.poll()) != null) {
				prom.setFailure(err);
			}
			messageList = null;
		}
	}

	public ResultFuture<HttpResponse> sendMessage(HttpRequest httpRequest) {
		if (ctx == null)
			throw new IllegalStateException();

		ResultFuture<HttpResponse> resultFuture = new ResultFuture<>();

		synchronized (this) {
			if (messageList == null) {
				// Connection closed
				resultFuture.setFailure(new IllegalStateException());
			} else if (messageList.offer(resultFuture)) {
				// Connection open and message accepted
				ChannelFuture channelFuture = ctx.getChannel().write(httpRequest);
				channelFuture.awaitUninterruptibly();
				if (!channelFuture.isSuccess()) {
					resultFuture.setFailure(channelFuture.getCause());
				}
				System.out.println("Request: " + httpRequest.getUri() );
			} else {
				// Connection open and message rejected
				resultFuture.setFailure(new BufferOverflowException());
			}
			return resultFuture;
		}
	}

	@Override
	public void messageReceived(
			ChannelHandlerContext ctx, MessageEvent e) throws Exception {
		if (messageList != null) {
			ResultFuture<HttpResponse> rf = messageList.poll();
			if (rf != null) {

				if (e.getMessage() instanceof HttpResponse) {
					rf.setResponse((HttpResponse) e.getMessage());
					System.out.println("Response " + ( ((HttpResponse) e.getMessage()).isChunked() ? "chunked" : "") + "\r\n" + ((HttpResponse)e.getMessage()).getContent().toString(StandardCharsets.UTF_8));
				}
			} else if (e.getMessage() instanceof HttpChunk) {
				HttpChunk chunk = (HttpChunk) e.getMessage();
				System.out.println("Chunk\r\n" +chunk.getContent().toString(StandardCharsets.UTF_8));

			}
		}
	}


	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e) throws Exception {
		super.exceptionCaught(ctx, e);
		if (messageList != null) {
			ResultFuture<HttpResponse> rf = messageList.poll();
			if (rf != null)
				rf.setFailure(e.getCause());
		}
	}
}
