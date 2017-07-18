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

import org.apache.zookeeper.dao.NodeDao;
import org.apache.zookeeper.dao.NodeDaoSqlite;
import org.apache.zookeeper.dao.NodeUpdateDaoSqlite;
import org.apache.zookeeper.dao.RecordNotFoundException;
import org.apache.zookeeper.bean.Node;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.service.NodeUpdateService;
import org.apache.zookeeper.service.NodeUpdateServiceImpl;
import org.apache.zookeeper.service.ZKWatchManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ZooKeeper {

	// TODO: inject or load from config
	final private NodeDao nodeDao = new NodeDaoSqlite("/home/natalia/test.db");
	final private NodeUpdateService nodeUpdateService;

	private Logger logger = LoggerFactory.getLogger(ZooKeeper.class.getName());
	final private WatchesNotifier watchesNotifier;
	final private ZKWatchManager watchManager;
	final private ExecutorService executor;

	/**
	 * Session associated with the client. Session is needed to identify Ephemeral nodes of the client and to remove
	 * them once session is closed.
	 */
	private long session;

	public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher) throws IOException {
		executor = Executors.newCachedThreadPool();
		session = new Random().nextLong();
		watchManager = new ZKWatchManager(watcher, false);
		watchesNotifier = new WatchesNotifier(watchManager);
		nodeUpdateService = new NodeUpdateServiceImpl(watchesNotifier,new NodeUpdateDaoSqlite("/home/natalia/test.db"));

		executor.submit(watchesNotifier);

//		watchesNotifier.send(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.SyncConnected, null));
		nodeUpdateService.processNodeEvent(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.SyncConnected, null));
	}

	public void close() {
		nodeDao.deleteEphemeralNodes(session);
		nodeUpdateService.processNodeEvent(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Disconnected, null));
	}

	/**
	 * Create a node with the given path and data.
	 * If a node with the same actual path already exists in the ZooKeeper, a
	 * KeeperException with error code KeeperException.NodeExists will be
	 * thrown.
	 * <p>
	 * If the parent node does not exist in the ZooKeeper, a KeeperException
	 * with error code KeeperException.NoNode will be thrown.
	 *
	 * @param path
	 * @param data
	 * @param acl
	 * @param createMode
	 * @return
	 */
	public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException {
		final Node node = new Node(path, data, createMode);
		node.setOwnerSession(session);

		try {
			Node parent = nodeDao.get(node.getParent().getPath());

			if(createMode.isSequential()){
				int cversion = parent.getCversion();
				path = path + cversion;
				parent.setCversion(cversion++);
				nodeDao.updateCVersion(parent);
				node.setPath(path);
			}

			if (nodeDao.create(node)) {
				sendNewNodeEvents(node);
				return node.getPath();
			}

			throw new KeeperException.NodeExistsException();
		} catch (RecordNotFoundException e) {
			throw new KeeperException.NoNodeException(node.getParent().getPath());
		}
	}


	public void create(final String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctx) {
		executor.submit(() ->
		{
			try {
				String node = create(path, data, acl, createMode);
				cb.processResult(KeeperException.Code.OK.intValue(), path, ctx, node);
			} catch (KeeperException e) {
				cb.processResult(e.code().intValue(), path, ctx, null);
			}
		});
	}

	/**
	 * Notifies all watches of the node path that new node was created.
	 * Notifies all watches of the parent node that node has new child.
	 *
	 * @param node
	 */
	private void sendNewNodeEvents(Node node) {
		processEvent(new WatchedEvent(Watcher.Event.EventType.NodeCreated, Watcher.Event.KeeperState.SyncConnected, node.getPath()));
		processEvent(new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, Watcher.Event.KeeperState.SyncConnected, node.getParent().getPath()));
	}

	/**
	 * Deletes the node with specified path and version.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown
	 * if the nodes does not exist.
	 * <p>
	 * A KeeperException with error code KeeperException.BadVersion will be
	 * thrown if the given version does not match the node's version.
	 * <p>
	 * A KeeperException with error code KeeperException.NotEmpty will be thrown
	 * if the node has children.
	 *
	 * @param path
	 * @param version
	 * @throws KeeperException
	 */
	public void delete(String path, int version) throws KeeperException {
		try {
			Node node = nodeDao.get(path);

			if (version >= 0 && node.getVersion() != version) {
				throw new KeeperException.BadVersionException(path);
			}

			List<String> children = nodeDao.getChildren(node);

			if (children.size() > 0) {
				throw new KeeperException.NotEmptyException(path);
			}

			node.setVersion(version);
			nodeDao.delete(node);

			processEvent(new WatchedEvent(Watcher.Event.EventType.NodeDeleted, Watcher.Event.KeeperState.SyncConnected, path));
			processEvent(new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, Watcher.Event.KeeperState.SyncConnected, node.getParent().getPath()));
		} catch (RecordNotFoundException r) {
			throw new KeeperException.NoNodeException(path);
		}
	}

	/**
	 * Returns the list of the children of the node of the given path.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown
	 * if no node with the given path exists.
	 *
	 * @param path
	 * @param watch
	 * @return
	 * @throws KeeperException
	 */
	public List<String> getChildren(String path, boolean watch) throws KeeperException {
		try {
			List<String> children = nodeDao.getChildren(new Node(path));

			if (watch) {
				WatchRegistration wr = new ChildWatchRegistration(watchManager.getDefaultWatcher(), path);
				wr.register(0);
			}

			return children;
		} catch (RecordNotFoundException e) {
			throw new KeeperException.NoNodeException(path);
		}
	}

	/**
	 * Return the data and the stat of the node of the given path.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown
	 * if no node with the given path exists.
	 *
	 * @param path
	 * @param watch
	 * @param stat
	 * @return
	 * @throws KeeperException
	 */
	public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException {
		Node node = getNode(path, watch);


		if (stat != null) {
			stat.setVersion(node.getVersion());
			stat.setCtime(node.getTimestamp());

			if (node.getMode() == CreateMode.EPHEMERAL) {
				stat.setEphemeralOwner(node.getOwnerSession());
			}
		}

		if (watch) {
			WatchRegistration wcb = new DataWatchRegistration(watchManager.getDefaultWatcher(), path);
			wcb.register(0);
		}
		return node.getData();
	}

	public void getData(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx) {
		executor.submit(() ->
		{
			try {
				Node node = getNode(path, watch);
				Stat stat = new Stat();
				stat.setCtime(node.getTimestamp());
				if (node.getMode() == CreateMode.EPHEMERAL) {
					stat.setEphemeralOwner(node.getOwnerSession());
				}

				if (watch) {
					WatchRegistration wcb = new DataWatchRegistration(watchManager.getDefaultWatcher(), path);
					wcb.register(0);
				}

				cb.processResult(KeeperException.Code.OK.intValue(), path, ctx, node.getData(), stat);
			} catch (KeeperException e) {
				cb.processResult(e.code().intValue(), path, ctx, null, new Stat());
			}
		});

	}

	private Node getNode(String path, boolean watch) throws KeeperException.NoNodeException {
		try {
			return nodeDao.get(path);
		} catch (RecordNotFoundException e) {
			throw new KeeperException.NoNodeException(path);
		}
	}

	/**
	 * Set the data for the node of the given path if such a node exists and the
	 * given version matches the version of the node (if the given version is
	 * -1, it matches any node's versions).
	 * <p>
	 *
	 * @param path
	 * @param data
	 * @param version
	 * @return The stat of the node
	 * @throws KeeperException
	 */
	public Stat setData(String path, byte[] data, int version) throws KeeperException {

		Stat stat = new Stat();

		try {
			Node node = nodeDao.get(path);

			if (version >= 0 && node.getVersion() != version) {
//				throw new KeeperException.BadVersionException(path);
				logger.warn(String.format( "Cached zkVersion [%d] not equal to that in zookeeper[%d] for path '%s'", version, node.getVersion(), path ) );
			}

			nodeDao.update(new Node(path, data, version));
			stat.setVersion(node.getVersion()+1);
		} catch (RecordNotFoundException e) {
			throw new KeeperException.NoNodeException(path);
		}
		processEvent( new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, Watcher.Event.KeeperState.SyncConnected, path));

		return stat;
	}

	/**
	 * Returns the stat of the node of the given path. Return null if no such a
	 * node exists.
	 *
	 * @param path
	 * @param watch
	 * @return
	 */
	public Stat exists(String path, boolean watch) {
		return exists(path, watch ? watchManager.getDefaultWatcher() : null);
	}

	public Stat exists(String path, Watcher watcher) {
		logger.info("Checking node " + path);

		boolean exists = false;
		Stat stat = null;
		try {
			Node node = nodeDao.get(path);
			exists = true;

			stat = new Stat();
			if (node.getMode() == CreateMode.EPHEMERAL) {
				stat.setEphemeralOwner(node.getOwnerSession());
			}
			stat.setVersion(node.getVersion());
			stat.setCversion(node.getCversion());
			stat.setMtime(node.getTimestamp());
			stat.setCtime(node.getTimestamp());
		} catch (RecordNotFoundException e) {
		}


		if (watcher != null) {
			WatchRegistration wcb = new ExistsWatchRegistration(watcher, path);
			wcb.register(0);
		}

		return stat;
	}

	public void setACL(String path, List<ACL> acl, int version, AsyncCallback.StatCallback cb, Object ctx) {
		throw new FakeZookeeperException(FakeZookeeperException.Code.UNIMPLEMENTED);
	}


	/**
	 * The asynchronous version of getChildren. Return the list of the children of the node of the given path.
	 *
	 * @param path
	 * @param watch
	 * @param cb
	 * @param ctx
	 */
	public void getChildren(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx) {
		executor.submit(() ->
		{
			try {
				List<String> children = getChildren(path, watch);

				if (watch) {
					WatchRegistration wr = new ChildWatchRegistration(watchManager.getDefaultWatcher(), path);
					wr.register(0);
				}

				cb.processResult(KeeperException.Code.OK.intValue(), path, ctx, children);
			} catch (KeeperException e) {
				cb.processResult(e.code().intValue(), path, ctx, null);
			}
		});
	}

	/**
	 * The session id for this ZooKeeper client instance.
	 *
	 * @return
	 */
	public long getSessionId() {
		return session;
	}


	private void processEvent(WatchedEvent event) {
		nodeUpdateService.processNodeEvent(event);
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

	/**
	 * Register a watcher for a particular path.
	 */
	abstract class WatchRegistration {
		private Watcher watcher;
		private String clientPath;

		public WatchRegistration(Watcher watcher, String clientPath) {
			this.watcher = watcher;
			this.clientPath = clientPath;
		}

		abstract protected Map<String, Set<Watcher>> getWatches(int rc);

		/**
		 * Register the watcher with the set of watches on path.
		 *
		 * @param rc the result code of the operation that attempted to
		 *           add the watch on the path.
		 */
		public void register(int rc) {
			if (shouldAddWatch(rc)) {
				Map<String, Set<Watcher>> watches = getWatches(rc);
				synchronized (watches) {
					Set<Watcher> watchers = watches.get(clientPath);
					if (watchers == null) {
						watchers = new HashSet<Watcher>();
						watches.put(clientPath, watchers);
					}
					watchers.add(watcher);
				}
			}
		}

		/**
		 * Determine whether the watch should be added based on return code.
		 *
		 * @param rc the result code of the operation that attempted to add the
		 *           watch on the node
		 * @return true if the watch should be added, otw false
		 */
		protected boolean shouldAddWatch(int rc) {
			return rc == 0;
		}
	}

	/**
	 * Handle the special case of exists watches - they add a watcher
	 * even in the case where NONODE result code is returned.
	 */
	class ExistsWatchRegistration extends WatchRegistration {
		public ExistsWatchRegistration(Watcher watcher, String clientPath) {
			super(watcher, clientPath);
		}

		@Override
		protected Map<String, Set<Watcher>> getWatches(int rc) {
			return rc == 0 ? watchManager.getDataWatches() : watchManager.getExistWatches();
		}

		@Override
		protected boolean shouldAddWatch(int rc) {
			return rc == 0 || rc == KeeperException.Code.NONODE.intValue();
		}
	}

	class DataWatchRegistration extends WatchRegistration {
		public DataWatchRegistration(Watcher watcher, String clientPath) {
			super(watcher, clientPath);
		}

		@Override
		protected Map<String, Set<Watcher>> getWatches(int rc) {
			return watchManager.getDataWatches();
		}
	}

	class ChildWatchRegistration extends WatchRegistration {
		public ChildWatchRegistration(Watcher watcher, String clientPath) {
			super(watcher, clientPath);
		}

		@Override
		protected Map<String, Set<Watcher>> getWatches(int rc) {
			return watchManager.getChildWatches();
		}
	}

}
