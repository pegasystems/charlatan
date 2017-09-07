package server;

import com.pega.zooikeeper.node.service.NodeService;
import com.pega.zooikeeper.node.service.NodeServiceImpl;
import com.pega.zooikeeper.utils.ZookeeperClassLoader;
import com.pega.zooikeeper.watches.service.WatchService;
import com.pega.zooikeeper.watches.service.WatchServiceImpl;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.*;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

public class ZookeeperNettyServer {
	static final ByteBuffer closeConn = ByteBuffer.allocate(0);
	private static final Logger log = LoggerFactory.getLogger(ZookeeperNettyServer.class);
	private static final int BIND_RETRY = 20;
	private static volatile ZookeeperNettyServer instance;
	private final String host;
	private int port;
	private Channel channel;
	private ServerBootstrap bootstrap;
	private NodeService nodeService;

	public ZookeeperNettyServer(String host, int port) {
		this.port = port;
		this.host = host;

		WatchService watchManager = new WatchServiceImpl(null, false);
		nodeService = new NodeServiceImpl(ZookeeperClassLoader.getNodeDao(), watchManager);
	}

	public static void main(String[] args) {

		org.apache.log4j.BasicConfigurator.configure();

		ZookeeperNettyServer server = new ZookeeperNettyServer("localhost", 2181);
		server.run();
	}

	public synchronized void run() {
		if (!isRunning()) {
			bootstrap = new ServerBootstrap(
					new NioServerSocketChannelFactory(
							Executors.newCachedThreadPool(),
							Executors.newCachedThreadPool()));
			bootstrap.setPipelineFactory(channelPipelineFactory(new CnxnChannelHandler()));
			// parent channel
			bootstrap.setOption("reuseAddress", true);
			// child channels
			bootstrap.setOption("child.tcpNoDelay", true);
		/* set socket linger to off, so that socket close does not block */
			bootstrap.setOption("child.soLinger", -1);

			for (; port < port + BIND_RETRY; port++) {
				InetSocketAddress address = new InetSocketAddress(host, port);
				try {
					channel = bootstrap.bind(address);
					log.info("Started data set server on {}:{}", host, port);
					break;
				} catch (ChannelException e) {
					log.warn("Cannot bind data set server to {}. Will try another port.", address);
					log.debug("Exception", e);
				}
			}
		} else {
			log.info("ZookeeperNettyServer is already running");
		}
	}

	public boolean isRunning() {
		return channel != null && channel.isOpen() && channel.isBound();
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
				instance = null;
			}
		}
		log.info("STOPPED sink server on {}:{}", host, port);
	}

	protected ChannelPipelineFactory channelPipelineFactory(final CnxnChannelHandler channelHandler) {
		return new ChannelPipelineFactory() {
			@Override
			public ChannelPipeline getPipeline() throws Exception {
				ChannelPipeline pipeline = Channels.pipeline();
				pipeline.addLast("servercnxnfactory", channelHandler);
				return pipeline;
			}
		};
	}

	class CnxnChannelHandler extends SimpleChannelHandler {

		private final AtomicLong msgCount = new AtomicLong();

		@Override
		public void channelClosed(ChannelHandlerContext ctx, ChannelStateEvent e)
				throws Exception {
			log.debug("Closed " + ctx.getChannel().getRemoteAddress());
		}

		@Override
		public void channelConnected(ChannelHandlerContext ctx,
									 ChannelStateEvent e) throws Exception {
			ZookeeperNettyConnection cnxn = new ZookeeperNettyConnection(ctx.getChannel(), nodeService);
			ctx.setAttachment(cnxn);

			log.debug("Connected " + ctx.getChannel().getRemoteAddress());
		}

		@Override
		public void channelDisconnected(ChannelHandlerContext ctx,
										ChannelStateEvent e) throws Exception {
			ZookeeperNettyConnection cnxn = (ZookeeperNettyConnection) ctx.getAttachment();
			if (cnxn != null) {
				cnxn.close();
			}

			log.debug("Disconnected " + ctx.getChannel().getRemoteAddress());
		}

		@Override
		public void exceptionCaught(ChannelHandlerContext ctx, ExceptionEvent e)
				throws Exception {
			ZookeeperNettyConnection cnxn = (ZookeeperNettyConnection) ctx.getAttachment();
			if (cnxn != null) {
				cnxn.close();
			}

			log.error("Exception " + ctx.getChannel().getRemoteAddress(), e.getCause());
		}

		@Override
		public void handleUpstream(ChannelHandlerContext ctx, ChannelEvent e) throws Exception {
			if (e instanceof ChannelStateEvent &&
					((ChannelStateEvent) e).getState() != ChannelState.INTEREST_OPS) {
				log.error(e.toString());
			}
			super.handleUpstream(ctx, e);
		}

		@Override
		public void messageReceived(ChannelHandlerContext ctx, MessageEvent e)
				throws Exception {
			try {
				ZookeeperNettyConnection cnxn = (ZookeeperNettyConnection) ctx.getAttachment();
				synchronized (cnxn) {
					processMessage(e, cnxn);
				}
			} catch (Exception ex) {
				throw ex;
			}
		}

		private void processMessage(MessageEvent e, ZookeeperNettyConnection cnxn) {
			ChannelBuffer buf = (ChannelBuffer) e.getMessage();
			cnxn.receiveMessage(buf);
		}


		@Override
		public void writeComplete(ChannelHandlerContext ctx,
								  WriteCompletionEvent e) throws Exception {
//			log.debug(msgCount + " write complete " + e);
		}
	}
}
