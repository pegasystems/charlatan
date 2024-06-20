/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper;

import com.pega.charlatan.broker.service.BrokerMonitorService;
import com.pega.charlatan.node.service.NodeService;
import com.pega.charlatan.node.service.NodeServiceImpl;
import com.pega.charlatan.server.session.bean.Session;
import com.pega.charlatan.utils.ZookeeperClassLoader;
import com.pega.charlatan.utils.ZookeeperNodeService;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ZooKeeper implements ZookeeperInterface {

	private static Logger logger = LoggerFactory.getLogger(ZooKeeper.class);
	private final ExecutorService executor;
	private ZookeeperNodeService nodeService;
	private BrokerMonitorService brokerMonitorService;
	private long session;
	private Watcher defaultWatcher;

	public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher) throws IOException {

		if (connectString != null) {
			String[] address = connectString.split(":");
			System.setProperty("ZOOKEEPER_HOST", address[0]);
			System.setProperty("ZOOKEEPER_PORT", address[1]);
		}
		this.executor = Executors.newCachedThreadPool();

		// Generate session
		this.session = new Random().nextLong();
		this.defaultWatcher = watcher;

		Session session = new Session(UUID.randomUUID(), System.currentTimeMillis());
		session.setTimeout(sessionTimeout);

		brokerMonitorService = new BrokerMonitorService(session, ZookeeperClassLoader.getSessionDaoImpl(), this);

		NodeService ns = new NodeServiceImpl(
				ZookeeperClassLoader.getNodeDao(),
				ZookeeperClassLoader.getWatchService());

		this.nodeService = new ZookeeperNodeService(ns);

		executor.submit(new Runnable() {
			@Override
			public void run() {
				defaultWatcher.process(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.SyncConnected, null));
			}
		});


		//Create root node if it doesn't exist
		try {
			create("/", null, null, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			//Ignore
		}
	}

	public void close() {
		close(session);
	}

	@Override
	public void close(long session) {
		nodeService.close(session);

		if (brokerMonitorService != null) {
			try {
				brokerMonitorService.stop();
			} catch (Exception e) {
				logger.error("Failed to stop broker monitor service", e);
			}
		}
	}


	/**
	 * Zookeper driver method
	 */
	@Override
	public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException {
		return create(session, path, data, acl, createMode);
	}

	@Override
	public String create(long session, String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException {
		checkIsBrokerInfo(path);
		return nodeService.create(session, path, data, acl, createMode);
	}

	/**
	 * Zookeper driver method
	 */
	@Override
	public void create(final String path, final byte[] data, final List<ACL> acl, final CreateMode createMode, final AsyncCallback.StringCallback cb, final Object ctx) {
		checkIsBrokerInfo(path);

		executor.submit(new Runnable() {
			@Override
			public void run() {
				try {
					String node = nodeService.create(session, path, data, acl, createMode);
					cb.processResult(KeeperException.Code.OK.intValue(), path, ctx, node);
				} catch (KeeperException e) {
					cb.processResult(e.code().intValue(), path, ctx, null);
				}
			}
		});
	}

	@Override
	public void delete(String path, int version) throws KeeperException {
		nodeService.delete(path, version);
	}

	/**
	 * Zookeper driver method
	 */
	@Override
	public List<String> getChildren(String path, boolean watch) throws KeeperException {
		return getChildren(path, watch ? defaultWatcher : null);
	}

	@Override
	public List<String> getChildren(String path, Watcher watcher) throws KeeperException {
		return nodeService.getChildren(path, watcher);
	}

	@Override
	public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException {
		return getData(path, watch ? defaultWatcher : null, stat);
	}

	@Override
	public byte[] getData(String path, Watcher watch, Stat stat) throws KeeperException {
		return nodeService.getData(path, watch, stat);
	}

	@Override
	public void getData(final String path, final boolean watch, final AsyncCallback.DataCallback cb, final Object ctx) {
		executor.submit(new Runnable() {
			@Override
			public void run() {
				try {
					Stat stat = new Stat();
					byte[] data = nodeService.getData(path, watch ? defaultWatcher : null, stat);

					cb.processResult(KeeperException.Code.OK.intValue(), path, ctx, data, stat);
				} catch (KeeperException e) {
					cb.processResult(e.code().intValue(), path, ctx, null, null);
				}
			}
		});
	}

	@Override
	public Stat setData(String path, byte[] data, int version) throws KeeperException {
		return nodeService.setData(path, data, version);
	}

	public Stat exists(String path, boolean watch) {
		return exists(path, watch ? defaultWatcher : null);
	}

	@Override
	public Stat exists(String path, Watcher watcher) {
		return nodeService.exists(path, watcher);
	}

	@Override
	public void setACL(String path, List<ACL> acl, int version, AsyncCallback.StatCallback cb, Object ctx) {
		logger.warn("Node ACL is unimplemented");
	}

	@Override
	public void getChildren(final String path, final boolean watch, final AsyncCallback.ChildrenCallback cb, final Object ctx) {
		executor.submit(new Runnable() {
			@Override
			public void run() {
				try {
					List<String> children = nodeService.getChildren(path, watch ? defaultWatcher : null);

					cb.processResult(KeeperException.Code.OK.intValue(), path, ctx, children);
				} catch (KeeperException e) {
					cb.processResult(e.code().intValue(), path, ctx, null);
				}
			}
		});
	}


	/**
	 * The session id for this ZooKeeper client instance.
	 *
	 * @return
	 */
	@Override
	public long getSessionId() {
		return session;
	}

	@Override
	public void removeEphemeralSessionNodes(long session) {
		nodeService.removeEphemeralSessionNodes(session);
	}

	@Override
	public void registerWatch(Watcher watcher, List<String> dataWatches, List<String> childWatches, List<String> existWatches) {
		nodeService.registerWatch(watcher, dataWatches, childWatches, existWatches);
	}

	private void checkIsBrokerInfo(String path) {
		// BrokerMonitorService can be initialized only when broker id is known
		if (!brokerMonitorService.isStarted()) {
			if (path.startsWith("/brokers/ids/")) {
				int brokerId = Integer.parseInt(path.substring(path.lastIndexOf("/") + 1));
				brokerMonitorService.start(brokerId, session);
			}
		}
	}

	public enum States {
		CONNECTING, ASSOCIATING, CONNECTED, CONNECTEDREADONLY,
		CLOSED, AUTH_FAILED, NOT_CONNECTED;

		public boolean isAlive() {
			return this != CLOSED && this != AUTH_FAILED;
		}

		/**
		 * Returns whether we are connected to a server (which
		 * could possibly be read-only, if this client is allowed
		 * to go to read-only mode)
		 */
		public boolean isConnected() {
			return this == CONNECTED || this == CONNECTEDREADONLY;
		}
	}
}
