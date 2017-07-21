package org.apache.zookeeper.impl.node.service;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.impl.common.ZookeeperClassLoader;
import org.apache.zookeeper.impl.node.bean.Node;
import org.apache.zookeeper.impl.node.dao.RecordNotFoundException;
import org.apache.zookeeper.impl.watches.service.ClientWatchManagerImpl;
import org.apache.zookeeper.impl.watches.service.RemoteNodeUpdates;
import org.apache.zookeeper.impl.watches.service.WatchesNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by natalia on 7/19/17.
 */
public class NodeServiceImpl implements NodeService {

	private final int sessionTimeout;
	private final ZKDatabase zkDatabase;
	private final WatchesNotifier watchesNotifier;
	private final ClientWatchManagerImpl watchManager;
	private final RemoteNodeUpdates remoteNodeUpdates;
	private final ExecutorService executor;
	private Logger logger = LoggerFactory.getLogger(ZooKeeper.class.getName());
	/**
	 * Session associated with the client. Session is needed to identify Ephemeral nodes of the client and to remove
	 * them once session is closed.
	 */
	private long session;

	public NodeServiceImpl(int sessionTimeout, Watcher watcher) throws IOException {
		// TODO: implement timeouts
		this.sessionTimeout = sessionTimeout;
		this.zkDatabase = new ZKDatabase(ZookeeperClassLoader.getNodeDao());

		// Generate session
		this.session = new Random().nextLong();

		watchManager = new ClientWatchManagerImpl(watcher, false);
		watchesNotifier = new WatchesNotifier(watchManager);

		remoteNodeUpdates = ZookeeperClassLoader.getRemoteNodeUpdates();
		remoteNodeUpdates.addNodeUpdateListener(watchesNotifier);

		executor = Executors.newCachedThreadPool();
		executor.submit(watchesNotifier);

		processEvent(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.SyncConnected, null));
	}


	@Override
	public void close() {
		List<String> ephemeralPaths = zkDatabase.getEphemeralPaths(session);
		for (String ephemeralPath : ephemeralPaths) {
			try {
				delete(ephemeralPath, -1);
			} catch (KeeperException e) {
				logger.error("Failed to remove session ephemeral node " + ephemeralPath, e);
			}
		}

		processEvent(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Disconnected, null));
	}

	@Override
	public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException {
		final Node node = new Node(path, data, createMode);

		try {
			Node parent = zkDatabase.get(node.getParentPath());
			int cversion = parent.getStat().getCversion();

			if (createMode.isSequential()) {
				//The number of changes to the children of this znode.
				path = path + String.format("%010d", cversion);
				node.setPath(path);
			}

			long now = System.currentTimeMillis();
			node.getStat().setCtime(now);
			node.getStat().setMtime(now);

			// small optimization: sequential branches are never created in the root
			if (!parent.isRoot()) {
				zkDatabase.updateCVersion(parent.getPath(), cversion + 1);
			}

			if (zkDatabase.create(session, node)) {
				sendNewNodeEvents(node);
				return node.getPath();
			}

			throw new KeeperException.NodeExistsException();
		} catch (RecordNotFoundException e) {
			throw new KeeperException.NoNodeException(node.getParentPath());
		}
	}


	@Override
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
		processEvent(new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, Watcher.Event.KeeperState.SyncConnected, node.getParentPath()));
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
	@Override
	public void delete(String path, int version) throws KeeperException {
		try {
			Node node = zkDatabase.get(path);

			if (version >= 0 && node.getStat().getVersion() != version) {
				throw new KeeperException.BadVersionException(path);
			}

			if (node.getChildren() != null && node.getChildren().size() > 0) {
				throw new KeeperException.NotEmptyException(path);
			}

			if (zkDatabase.delete(node)) {

				processEvent(new WatchedEvent(Watcher.Event.EventType.NodeDeleted, Watcher.Event.KeeperState.SyncConnected, path));
				processEvent(new WatchedEvent(Watcher.Event.EventType.NodeChildrenChanged, Watcher.Event.KeeperState.SyncConnected, node.getParentPath()));
			}
			//node was modificated during the deletion process
			else {
				throw new KeeperException.BadVersionException(path);
			}
		} catch (RecordNotFoundException r) {
			throw new KeeperException.NoNodeException(path);
		}
	}

	@Override
	public List<String> getChildren(String path, boolean watch) throws KeeperException {
		try {

			Node node = zkDatabase.get(path);

			if (watch) {
				WatchRegistration wr = new ChildWatchRegistration(watchManager.getDefaultWatcher(), path);
				wr.register(0);
			}

			return node.getChildren();
		} catch (RecordNotFoundException e) {
			throw new KeeperException.NoNodeException(path);
		}
	}


	@Override
	public void getChildren(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx) {
		executor.submit(() ->
		{
			try {
				List<String> children = getChildren(path, watch);

				cb.processResult(KeeperException.Code.OK.intValue(), path, ctx, children);
			} catch (KeeperException e) {
				cb.processResult(e.code().intValue(), path, ctx, null);
			}
		});
	}

	@Override
	public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException {
		Node node = getNode(path);


		if (stat != null) {
			stat.loadFrom(node.getStat());
		}

		if (watch) {
			WatchRegistration wcb = new DataWatchRegistration(watchManager.getDefaultWatcher(), path);
			wcb.register(0);
		}
		return node.getData();
	}

	@Override
	public void getData(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx) {
		executor.submit(() ->
		{
			try {
				Node node = getNode(path);

				if (watch) {
					WatchRegistration wcb = new DataWatchRegistration(watchManager.getDefaultWatcher(), path);
					wcb.register(0);
				}

				Stat stat = new Stat(node.getStat());

				cb.processResult(KeeperException.Code.OK.intValue(), path, ctx, node.getData(), stat);
			} catch (KeeperException e) {
				cb.processResult(e.code().intValue(), path, ctx, null, null);
			}
		});

	}

	private Node getNode(String path) throws KeeperException.NoNodeException {
		try {
			return zkDatabase.get(path);
		} catch (RecordNotFoundException e) {
			throw new KeeperException.NoNodeException(path);
		}
	}

	@Override
	public Stat setData(String path, byte[] data, int version) throws KeeperException {


		Node node = getNode(path);

		if (version >= 0 && node.getStat().getVersion() != version) {
			throw new KeeperException.BadVersionException(path);
//				logger.warn(String.format("Cached zkVersion [%d] not equal to that in zookeeper[%d] for path '%s'", version, node.getVersion(), path));
		}

		zkDatabase.update(path, data, version + 1, System.currentTimeMillis());

		node = getNode(path);

		processEvent(new WatchedEvent(Watcher.Event.EventType.NodeDataChanged, Watcher.Event.KeeperState.SyncConnected, path));

		return new Stat(node.getStat());
	}

	/**
	 * Returns the stat of the node of the given path. Return null if no such a
	 * node exists.
	 *
	 * @param path
	 * @param watch
	 * @return
	 */
	@Override
	public Stat exists(String path, boolean watch) {
		return exists(path, watch ? watchManager.getDefaultWatcher() : null);
	}

	@Override
	public Stat exists(String path, Watcher watcher) {
		logger.info("Checking node " + path);

		Stat stat = null;
		try {
			Node node = zkDatabase.get(path);
			stat = new Stat(node.getStat());

		} catch (RecordNotFoundException e) {
		}

		if (watcher != null) {
			WatchRegistration wcb = new ExistsWatchRegistration(watcher, path);
			wcb.register(0);
		}

		return stat;
	}

	@Override
	public void setACL(String path, List<ACL> acl, int version, AsyncCallback.StatCallback cb, Object ctx) {
		logger.warn("Node ACL is unimplemented");
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

	private void processEvent(WatchedEvent event) {
		if(event.getPath() != null ) {
			remoteNodeUpdates.processLocalWatchedEvent(event);
		}
		watchesNotifier.processWatchedEvent(event);
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