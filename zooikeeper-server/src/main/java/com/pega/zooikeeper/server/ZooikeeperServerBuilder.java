package com.pega.zooikeeper.server;

import com.pega.zooikeeper.node.dao.NodeDao;
import com.pega.zooikeeper.watches.service.WatchService;
import org.jboss.netty.channel.socket.nio.NioWorker;
import org.jboss.netty.channel.socket.nio.WorkerPool;

public class ZooikeeperServerBuilder {

	private int port;
	private String host;
	private boolean secure;
	private WorkerPool<NioWorker> workerPool;

	private NodeDao nodeDao;
	private WatchService watchService;

	public int getPort() {
		return port;
	}

	public ZooikeeperServerBuilder setPort(int port) {
		this.port = port;
		return this;
	}

	public String getHost() {
		return host;
	}

	public ZooikeeperServerBuilder setHost(String host) {
		this.host = host;
		return this;
	}

	public boolean isSecure() {
		return secure;
	}

	public ZooikeeperServerBuilder setSecure(boolean secure) {
		this.secure = secure;
		return this;
	}

	public WorkerPool<NioWorker> getWorkerPool() {
		return workerPool;
	}

	public ZooikeeperServerBuilder setWorkerPool(WorkerPool<NioWorker> workerPool) {
		this.workerPool = workerPool;
		return this;
	}

	public NodeDao getNodeDao() {
		return nodeDao;
	}

	public ZooikeeperServerBuilder setNodeDao(NodeDao nodeDao) {
		this.nodeDao = nodeDao;
		return this;
	}

	public WatchService getNodeUpdateDao() {
		return watchService;
	}

	public ZooikeeperServerBuilder setWatchService(WatchService watchService) {
		this.watchService = watchService;
		return this;
	}

	public ZooikeeperNettyServer build() {
		if (host == null) {
			throw new IllegalStateException("Zooikeeper host is not set");
		}
		if (port <= 0) {
			throw new IllegalStateException("Zooikeeper port is not set");
		}
		if (workerPool == null) {
			throw new IllegalStateException("Zooikeeper worker pool is not set");
		}
		if (nodeDao == null) {
			throw new IllegalStateException("Zooikeeper node dao is not set");
		}
		if (watchService == null) {
			throw new IllegalStateException("Zooikeeper watch service is not set");
		}

		return new ZooikeeperNettyServer(this);
	}
}
