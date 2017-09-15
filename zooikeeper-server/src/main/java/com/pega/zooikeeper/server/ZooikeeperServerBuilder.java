package com.pega.zooikeeper.server;

import com.pega.zooikeeper.node.dao.NodeDao;
import com.pega.zooikeeper.server.session.service.SessionService;
import com.pega.zooikeeper.watches.service.WatchService;

import java.util.concurrent.ThreadFactory;

public class ZooikeeperServerBuilder {

	private int port;
	private String host;
	private String id;
	private boolean secure;
	private ThreadFactory threadFactory;
	private int workerCount;
	private NodeDao nodeDao;
	private WatchService watchService;
	private SessionService sessionService;

	private int maxSessionTimeout;

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

	public ThreadFactory getThreadFactory() {
		return threadFactory;
	}

	public ZooikeeperServerBuilder setThreadFactory(ThreadFactory threadFactory) {
		this.threadFactory = threadFactory;
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

	public int getMaxSessionTimeout() {
		return maxSessionTimeout;
	}

	public ZooikeeperServerBuilder setMaxSessionTimeout(int maxSessionTimeout) {
		this.maxSessionTimeout = maxSessionTimeout;
		return this;
	}

	public SessionService getSessionService() {
		return sessionService;
	}

	public ZooikeeperServerBuilder setSessionService(SessionService sessionService) {
		this.sessionService = sessionService;
		return this;
	}

	public String getId() {
		return id;
	}

	public ZooikeeperServerBuilder setId(String id) {
		this.id = id;
		return this;
	}


	public int getWorkerCount() {
		return workerCount;
	}

	public ZooikeeperServerBuilder setWorkerCount(int workerCount) {
		this.workerCount = workerCount;
		return this;
	}

	public ZooikeeperNettyServer build() {
		if (host == null) {
			throw new IllegalStateException("Zooikeeper host is not set");
		}
		if (port <= 0) {
			throw new IllegalStateException("Zooikeeper port is not set");
		}
		if (threadFactory == null) {
			throw new IllegalStateException("Zooikeeper thread factory is not set");
		}
		if (nodeDao == null) {
			throw new IllegalStateException("Zooikeeper node dao is not set");
		}
		if (watchService == null) {
			throw new IllegalStateException("Zooikeeper watch service is not set");
		}
		if (sessionService == null) {
			throw new IllegalStateException("Zooikeeper session service is not set");
		}
		if (workerCount <= 0) {
			throw new IllegalStateException("Zooikeeper worker count is not set");
		}
		return new ZooikeeperNettyServer(this);
	}
}
