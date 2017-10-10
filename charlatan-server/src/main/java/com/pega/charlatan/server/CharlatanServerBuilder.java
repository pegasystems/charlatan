package com.pega.charlatan.server;

import com.pega.charlatan.node.dao.NodeDao;
import com.pega.charlatan.server.session.service.SessionService;
import com.pega.charlatan.watches.service.WatchService;

import java.util.concurrent.ThreadFactory;

public class CharlatanServerBuilder {

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

	public CharlatanServerBuilder setPort(int port) {
		this.port = port;
		return this;
	}

	public String getHost() {
		return host;
	}

	public CharlatanServerBuilder setHost(String host) {
		this.host = host;
		return this;
	}

	public boolean isSecure() {
		return secure;
	}

	public CharlatanServerBuilder setSecure(boolean secure) {
		this.secure = secure;
		return this;
	}

	public ThreadFactory getThreadFactory() {
		return threadFactory;
	}

	public CharlatanServerBuilder setThreadFactory(ThreadFactory threadFactory) {
		this.threadFactory = threadFactory;
		return this;
	}

	public NodeDao getNodeDao() {
		return nodeDao;
	}

	public CharlatanServerBuilder setNodeDao(NodeDao nodeDao) {
		this.nodeDao = nodeDao;
		return this;
	}

	public WatchService getNodeUpdateDao() {
		return watchService;
	}

	public CharlatanServerBuilder setWatchService(WatchService watchService) {
		this.watchService = watchService;
		return this;
	}

	public int getMaxSessionTimeout() {
		return maxSessionTimeout;
	}

	public CharlatanServerBuilder setMaxSessionTimeout(int maxSessionTimeout) {
		this.maxSessionTimeout = maxSessionTimeout;
		return this;
	}

	public SessionService getSessionService() {
		return sessionService;
	}

	public CharlatanServerBuilder setSessionService(SessionService sessionService) {
		this.sessionService = sessionService;
		return this;
	}

	public String getId() {
		return id;
	}

	public CharlatanServerBuilder setId(String id) {
		this.id = id;
		return this;
	}


	public int getWorkerCount() {
		return workerCount;
	}

	public CharlatanServerBuilder setWorkerCount(int workerCount) {
		this.workerCount = workerCount;
		return this;
	}

	public CharlatanNettyServer build() {
		if (host == null) {
			throw new IllegalStateException("Charlatan host is not set");
		}
		if (port <= 0) {
			throw new IllegalStateException("Charlatan port is not set");
		}
		if (threadFactory == null) {
			throw new IllegalStateException("Charlatan thread factory is not set");
		}
		if (nodeDao == null) {
			throw new IllegalStateException("Charlatan node dao is not set");
		}
		if (watchService == null) {
			throw new IllegalStateException("Charlatan watch service is not set");
		}
		if (sessionService == null) {
			throw new IllegalStateException("Charlatan session service is not set");
		}
		if (workerCount <= 0) {
			throw new IllegalStateException("Charlatan worker count is not set");
		}
		return new CharlatanNettyServer(this);
	}
}
