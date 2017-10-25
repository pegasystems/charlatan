package com.pega.charlatan.server;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pega.charlatan.node.dao.NodeDao;
import com.pega.charlatan.node.service.NodeService;
import com.pega.charlatan.node.service.NodeServiceImpl;
import com.pega.charlatan.server.session.bean.Session;
import com.pega.charlatan.server.session.service.SessionService;
import com.pega.charlatan.watches.service.WatchService;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.LengthFieldBasedFrameDecoder;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.Attribute;
import io.netty.util.AttributeKey;
import io.netty.util.concurrent.Future;

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

		EventLoopGroup bossGroup = new NioEventLoopGroup();
	    EventLoopGroup workerGroup = new NioEventLoopGroup(workerCount, threadFactory);
		
		bootstrap = new ServerBootstrap();
		bootstrap.group(bossGroup, workerGroup);
		bootstrap.channel(NioServerSocketChannel.class);
		
		bootstrap.option(ChannelOption.SO_REUSEADDR, true);
		bootstrap.childOption(ChannelOption.TCP_NODELAY, true);
		// set socket linger to off, so that socket close does not block
		bootstrap.childOption(ChannelOption.SO_LINGER, -1);

		bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
			@Override
			protected void initChannel(SocketChannel channel) throws Exception {
				logger.info("Initializing socket channel for connection from remote address [{}]", channel.remoteAddress());
				channel.pipeline()
					.addLast("IdleHandler", new IdleStateHandler(DEFAULT_MAX_SESSION_TIMEOUT, 0, 0, TimeUnit.MILLISECONDS))
					.addLast("FrameDecoder", new LengthFieldBasedFrameDecoder(65536, 0, 4, 0, 0))
					.addLast("handler", new CharlatanChannelHandler());
			}
		});

		nodeService = new NodeServiceImpl(nodeDao, watchService);
	}

	public synchronized void start() {
		if (!isRunning()) {

			// Try to create root node.
			try {
				nodeService.create(0, "/", null, null, CreateMode.PERSISTENT);
			} catch (KeeperException ignore) {
				// Ignore if root node already exists
			}

			invalidateStaleSessions();

			sessionMonitorService.scheduleAtFixedRate(new Runnable() {
				@Override
				public void run() {
					invalidateStaleSessions();
				}
			}, maxSessionTimeout / 2, maxSessionTimeout / 2, TimeUnit.MILLISECONDS);

			try {
				channel = bootstrap.bind(host, port).sync().channel();
			} catch (InterruptedException e) {
				logger.error("Interrupted");
			}

			logger.info("Started Charlatan server on {}:{}", host, port);
		} else {
			logger.info("Charlatan server is already running");
		}
	}

	public synchronized void stop() {
		if (isRunning()) {
			Future<?> bossGroupShutdownFuture = bootstrap.group().shutdownGracefully();
			Future<?> childGroupShutdownFuture = bootstrap.childGroup().shutdownGracefully();
			
			try {
				bossGroupShutdownFuture.await();
			} catch(InterruptedException ex) {
				logger.warn("Interrupted waiting for netty boss event loop group shutdown");
			}
			
			try {
				childGroupShutdownFuture.await();
			} catch(InterruptedException ex) {
				logger.warn("Interrupted waiting for netty worker event loop group shutdown");
			}
		}
		sessionMonitorService.shutdown();

		logger.info("Stopped Charlatan server on {}:{}", host, port);
	}

	public synchronized boolean isRunning() {
		return channel != null && channel.isOpen();
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
	
	class CharlatanChannelHandler extends  SimpleChannelInboundHandler<ByteBuf> {
		
		private static final String CONNECTION_ATTR = "connection";
		
		private final AttributeKey<CharlatanNettyConnection> CONNECTION = AttributeKey.valueOf("connection");
		
		@Override
		public void channelActive(ChannelHandlerContext ctx) throws Exception {
			super.channelActive(ctx);
			
			Session session = new Session(UUID.randomUUID(), System.currentTimeMillis());
			CharlatanNettyConnection cnxn = new CharlatanNettyConnection(ctx.channel(), nodeService, session);
			Attribute<CharlatanNettyConnection> attr = ctx.attr(CONNECTION);
			attr.set(cnxn);

			sessions.put(session.getUuid(), cnxn);
			sessionService.registerSession(id, session);

			logger.info("Connection established from address [{}]", ctx.channel().remoteAddress());
			
		}

		@Override
		public void channelInactive(ChannelHandlerContext ctx) throws Exception {
			super.channelInactive(ctx);
			Attribute<CharlatanNettyConnection> attr = ctx.attr(CONNECTION);
			CharlatanNettyConnection cnxn = attr.get();
			if (cnxn != null) {
				Session session = cnxn.getSession();

				logger.debug("Cleaning session connection " + session);
				cnxn.close();

				logger.debug("Deleting session info " + session);
				sessionService.deleteSession(session.getUuid());

				sessions.remove(session.getUuid());
			}

			logger.info("Connection disconnected from address [{}]", ctx.channel().remoteAddress());
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
			super.exceptionCaught(ctx, cause);
			Attribute<CharlatanNettyConnection> attr = ctx.attr(CONNECTION);
			CharlatanNettyConnection cnxn = attr.get();
			if (cnxn != null) {
				cnxn.close();
			}

			logger.error("Exception " + ctx.channel().remoteAddress(), cause.getCause());
		}

		@Override
		protected void channelRead0(ChannelHandlerContext ctx, ByteBuf msg) throws Exception {
			logger.debug("Received [{}] message bytes from address [{}]", msg.readableBytes(), ctx.channel().remoteAddress());
			Attribute<CharlatanNettyConnection> attr = ctx.attr(CONNECTION);
			CharlatanNettyConnection cnxn = attr.get();
			cnxn.receiveMessage(msg);
			sessionService.updateSession(cnxn.getSession());
		}

		@Override
		public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
			if (evt instanceof IdleStateEvent && ((IdleStateEvent)evt).state() == IdleState.READER_IDLE) {
				logger.info("Recieved inactivity event from netty stack for remote address [{}]", ctx.channel().remoteAddress());
				ctx.close();
			}
		}
	}
}
