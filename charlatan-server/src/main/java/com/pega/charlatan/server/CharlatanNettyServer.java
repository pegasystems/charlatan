package com.pega.charlatan.server;

import com.pega.charlatan.node.dao.NodeDao;
import com.pega.charlatan.node.service.NodeService;
import com.pega.charlatan.node.service.NodeServiceImpl;
import com.pega.charlatan.server.session.bean.Session;
import com.pega.charlatan.server.session.service.SessionService;
import com.pega.charlatan.watches.service.WatchService;
import com.pega.charlatan.node.bean.CreateMode;
import com.pega.charlatan.utils.CharlatanException;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

public class CharlatanNettyServer {

	public static final int DEFAULT_MAX_SESSION_TIMEOUT = 60000;
	private static final Logger logger = LoggerFactory.getLogger(CharlatanNettyServer.class);
	private final String host;
	private final int port;

	//Server identifier (optional)
	private final String id;

	//TODO: implement secure connection
	private final boolean secure;
	private final int maxSessionTimeout;

	private Channel channel;
	private ServerBootstrap bootstrap;
	private NodeService nodeService;

	private NodeDao nodeDao;
	private WatchService watchService;

	private SessionService sessionService;
	private ScheduledExecutorService sessionMonitorService;

	private ThreadFactory threadFactory;
	private int workerCount;

	private Map<UUID, CharlatanNettyConnection> sessions;

	CharlatanNettyServer(CharlatanServerBuilder builder) {
		this.host = builder.getHost();
		this.port = builder.getPort();
		this.secure = builder.isSecure();
		this.nodeDao = builder.getNodeDao();
		this.watchService = builder.getNodeUpdateDao();
		this.id = builder.getId();

		if (builder.getMaxSessionTimeout() > 0) {
			this.maxSessionTimeout = builder.getMaxSessionTimeout();
		} else {
			this.maxSessionTimeout = DEFAULT_MAX_SESSION_TIMEOUT;
		}

		this.threadFactory = builder.getThreadFactory();
		this.workerCount = builder.getWorkerCount();

		this.sessions = new HashMap<>();
		this.sessionService = builder.getSessionService();
		this.sessionMonitorService = Executors.newSingleThreadScheduledExecutor(threadFactory);

		bootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory(
						Executors.newCachedThreadPool(),
						new NioWorkerPool(Executors.newCachedThreadPool(threadFactory), workerCount)));

		// parent channel
		bootstrap.setOption("reuseAddress", true);
		// child channels
		bootstrap.setOption("child.tcpNoDelay", true);
		// set socket linger to off, so that socket close does not block
		bootstrap.setOption("child.soLinger", -1);
		bootstrap.setPipelineFactory(new PipelineFactory(new CharlatanChannelHandler()));

		nodeService = new NodeServiceImpl(nodeDao, watchService);
	}

	public synchronized void start() {
		if (!isRunning()) {

			// Try to create root node.
			try {
				nodeService.create(0, "/", null, CreateMode.PERSISTENT);
			} catch (CharlatanException ignore) {
				// Ignore if root node already exists
			}

			invalidateStaleSessions();

			sessionMonitorService.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					invalidateStaleSessions();
				}
			}, maxSessionTimeout / 2, maxSessionTimeout / 2, TimeUnit.MILLISECONDS);

			InetSocketAddress address = new InetSocketAddress(host, port);
			channel = bootstrap.bind(address);

			logger.info("Started Charlatan server on {}:{}", host, port);
		} else {
			logger.info("Charlatan server is already running");
		}
	}

	public synchronized void stop() {
		ChannelFuture closeFuture = null;
		try {
			if (isRunning()) {
				channel.unbind();
				closeFuture = channel.close();
			}
			if (bootstrap != null) {
				bootstrap.releaseExternalResources();
			}

			sessionMonitorService.shutdown();
		} finally {
			channel = null;
			if (closeFuture != null) {
				closeFuture.awaitUninterruptibly();
			}
		}
		logger.info("Stopped Charlatan server on {}:{}", host, port);
	}

	public synchronized boolean isRunning() {
		return channel != null && channel.isOpen() && channel.isBound();
	}

	private void invalidateStaleSessions() {
		try {
			List<Session> staleSessions = sessionService.getStaleSessions(System.currentTimeMillis() - maxSessionTimeout );

			for (Session session : staleSessions) {
				CharlatanNettyConnection connection = sessions.get(session.getUuid());
				if (connection != null) {
					// This should never happen, stale connection is still present but it wasn't closed by timeout
					logger.error(String.format("Found stale sessions %d on current server", session.getSessionId()));
					connection.close();
				} else {
					logger.info(String.format("Found stale session %d, invalidating the session", session.getSessionId()));
					nodeService.close(session.getSessionId());
					logger.debug("Deleting session info " + session);
					sessionService.deleteSession(session.getUuid());
				}
			}
		} catch (Throwable e) {
			logger.warn("Failed to invalidate stale brokers", e);
		}
	}


	class PipelineFactory implements ChannelPipelineFactory {
		private final ChannelHandler handler;

		public PipelineFactory(ChannelHandler handler) {
			this.handler = handler;
		}

		@Override
		public ChannelPipeline getPipeline() throws Exception {
			ChannelPipeline pipeline = Channels.pipeline();
			pipeline.addLast("FrameDecoder", new LengthFieldBasedFrameDecoder(65536, 0, 4, 0, 0));
			pipeline.addLast("handler", handler);
			return pipeline;
		}
	}

	class CharlatanChannelHandler extends SimpleChannelHandler {

		@Override
		public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
				throws Exception {
			logger.debug("Closed " + ctx.getChannel().getRemoteAddress());
		}

		@Override
		public void channelConnected(ChannelHandlerContext ctx,
									 ChannelStateEvent e) throws Exception {

			Session session = new Session(UUID.randomUUID(), System.currentTimeMillis());
			CharlatanNettyConnection cnxn = new CharlatanNettyConnection(ctx.getChannel(), nodeService, session);
			ctx.setAttachment(cnxn);

			sessions.put(session.getUuid(), cnxn);
			sessionService.registerSession(id, session);

			logger.debug("Connected " + ctx.getChannel().getRemoteAddress());
		}

		@Override
		public void channelDisconnected(ChannelHandlerContext ctx,
										ChannelStateEvent e) throws Exception {
			CharlatanNettyConnection cnxn = (CharlatanNettyConnection) ctx.getAttachment();
			if (cnxn != null) {
				Session session = cnxn.getSession();

				logger.debug("Cleaning session connection " + session);
				cnxn.close();

				logger.debug("Deleting session info " + session);
				sessionService.deleteSession(session.getUuid());

				sessions.remove(session.getUuid());
			}

			logger.debug("Disconnected " + ctx.getChannel().getRemoteAddress());
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
				throws Exception {
			CharlatanNettyConnection cnxn = (CharlatanNettyConnection) ctx.getAttachment();
			if (cnxn != null) {
				cnxn.close();
			}

			logger.error("Exception " + ctx.getChannel().getRemoteAddress(), e.getCause());
		}

		@Override
		public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
			if (e instanceof ChannelStateEvent &&
					((ChannelStateEvent) e).getState() != ChannelState.INTEREST_OPS) {
				logger.error(e.toString());
			}
			super.handleUpstream(ctx, e);
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
				throws Exception {
			try {
				CharlatanNettyConnection connection = (CharlatanNettyConnection) ctx.getAttachment();
				synchronized (connection) {
					processMessage(e, connection);
				}
			} catch (Exception ex) {
				throw ex;
			}
		}

		private void processMessage(MessageEvent e, CharlatanNettyConnection cnxn) {
			ChannelBuffer buf = (ChannelBuffer) e.getMessage();
			cnxn.receiveMessage(buf);
			sessionService.updateSession(cnxn.getSession());
		}
	}
}
