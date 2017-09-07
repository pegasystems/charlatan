package com.pega.zooikeeper.server;

import com.pega.zooikeeper.node.dao.NodeDao;
import com.pega.zooikeeper.node.service.NodeService;
import com.pega.zooikeeper.node.service.NodeServiceImpl;
import com.pega.zooikeeper.watches.service.WatchCacheImpl;
import com.pega.zooikeeper.watches.service.WatchService;
import com.pega.zooikeeper.watches.service.WatchServiceImpl;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.channel.socket.nio.NioWorkerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.util.concurrent.Executors;

public class ZooikeeperNettyServer {

	private static final Logger logger = LoggerFactory.getLogger(ZooikeeperNettyServer.class);

	private final String host;
	private final int port;

	//TODO: implement secure connection
	private final boolean secure;

	private Channel channel;
	private ServerBootstrap bootstrap;
	private NodeService nodeService;

	private NodeDao nodeDao;
	private WatchService watchService;

	ZooikeeperNettyServer(ZooikeeperServerBuilder builder) {
		this.host = builder.getHost();
		this.port = builder.getPort();
		this.secure = builder.isSecure();
		this.nodeDao = builder.getNodeDao();
		this.watchService = builder.getNodeUpdateDao();

		bootstrap = new ServerBootstrap(
				new NioServerSocketChannelFactory(
						Executors.newCachedThreadPool(),
						builder.getWorkerPool()));

		// parent channel
		bootstrap.setOption("reuseAddress", true);
		// child channels
		bootstrap.setOption("child.tcpNoDelay", true);
		// set socket linger to off, so that socket close does not block
		bootstrap.setOption("child.soLinger", -1);
		bootstrap.setPipelineFactory(new PipelineFactory(new ZooikeeperChannelHandler()));

		nodeService = new NodeServiceImpl(nodeDao, watchService);
	}

	public static void main(String[] args) {

		org.apache.log4j.BasicConfigurator.configure();

		ZooikeeperNettyServer server = new ZooikeeperServerBuilder()
				.setHost("localhost")
				.setPort(2181)
				.setWorkerPool(new NioWorkerPool(Executors.newCachedThreadPool(), 5))
				.setSecure(true)
				.setNodeDao(new com.pega.zooikeeper.sqlite.NodeDaoSqlite())
				.setWatchService(new WatchServiceImpl(new WatchCacheImpl(), new com.pega.zooikeeper.sqlite.NodeUpdateDaoSqlite()))
				.build();

		server.start();
	}

	public synchronized void start() {
		if (!isRunning()) {
			InetSocketAddress address = new InetSocketAddress(host, port);
			channel = bootstrap.bind(address);

			logger.info("Started Zooikeeper server on {}:{}", host, port);
		} else {
			logger.info("Zooikeeper server is already running");
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
		} finally {
			channel = null;
			if (closeFuture != null) {
				closeFuture.awaitUninterruptibly();
			}
		}
		logger.info("Stopped Zooikeeper server on {}:{}", host, port);
	}

	public boolean isRunning() {
		return channel != null && channel.isOpen() && channel.isBound();
	}

	class PipelineFactory implements ChannelPipelineFactory {
		private final ChannelHandler handler;

		public PipelineFactory(ChannelHandler handler) {
			this.handler = handler;
		}

		@Override
		public ChannelPipeline getPipeline() throws Exception {
			ChannelPipeline pipeline = Channels.pipeline();
			pipeline.addLast("handler", handler);
			return pipeline;
		}
	}

	;

	class ZooikeeperChannelHandler extends SimpleChannelHandler {

		@Override
		public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
				throws Exception {
			logger.debug("Closed " + ctx.getChannel().getRemoteAddress());
		}

		@Override
		public void channelConnected(ChannelHandlerContext ctx,
									 ChannelStateEvent e) throws Exception {
			ZooikeeperNettyConnection cnxn = new ZooikeeperNettyConnection(ctx.getChannel(), nodeService);
			ctx.setAttachment(cnxn);

			logger.debug("Connected " + ctx.getChannel().getRemoteAddress());
		}

		@Override
		public void channelDisconnected(ChannelHandlerContext ctx,
										ChannelStateEvent e) throws Exception {
			ZooikeeperNettyConnection cnxn = (ZooikeeperNettyConnection) ctx.getAttachment();
			if (cnxn != null) {
				cnxn.close();
			}

			logger.debug("Disconnected " + ctx.getChannel().getRemoteAddress());
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
				throws Exception {
			ZooikeeperNettyConnection cnxn = (ZooikeeperNettyConnection) ctx.getAttachment();
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
				ZooikeeperNettyConnection cnxn = (ZooikeeperNettyConnection) ctx.getAttachment();
				synchronized (cnxn) {
					processMessage(e, cnxn);
				}
			} catch (Exception ex) {
				throw ex;
			}
		}

		private void processMessage(MessageEvent e, ZooikeeperNettyConnection cnxn) {
			ChannelBuffer buf = (ChannelBuffer) e.getMessage();
			cnxn.receiveMessage(buf);
		}
	}
}
