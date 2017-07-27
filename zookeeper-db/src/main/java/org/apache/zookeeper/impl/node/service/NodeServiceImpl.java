package org.apache.zookeeper.impl.node.service;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.impl.broker.service.BrokerMonitorService;
import org.apache.zookeeper.impl.common.ZookeeperClassLoader;
import org.apache.zookeeper.impl.node.bean.Node;
import org.apache.zookeeper.impl.node.dao.RecordNotFoundException;
import org.apache.zookeeper.impl.watches.service.ClientWatchManager;
import org.apache.zookeeper.impl.watches.service.ClientWatchManagerImpl;
import org.apache.zookeeper.impl.watches.service.RemoteNodeUpdates;
import org.apache.zookeeper.impl.watches.service.WatchesNotifier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by natalia on 7/19/17.
 */
public class NodeServiceImpl implements NodeService {

	private final int sessionTimeout;
	private final ZKDatabase zkDatabase;
	private final WatchesNotifier watchesNotifier;
	private final ClientWatchManager watchManager;
	private final RemoteNodeUpdates remoteNodeUpdates;
	private final ExecutorService executor;

	private BrokerMonitorService brokerMonitorService;
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

		removeSessionNodes(session);

		if (brokerMonitorService != null) {
			try {
				brokerMonitorService.stop();
			} catch (Exception e) {
				logger.error("Failed to stop broker monitor service", e);
			}
		}

		processEvent(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Disconnected, null));
	}

	@Override
	public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException {
		checkIsBrokerInfo(path);

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

	private void checkIsBrokerInfo(String path) {
		// BrokerMonitorService can be initialized only when broker id is known
		if (brokerMonitorService == null) {
			if (path.startsWith("/brokers/ids/")) {
				synchronized (this) {
					if (brokerMonitorService == null) {
						int brokerId = Integer.parseInt(path.substring(path.lastIndexOf("/") + 1));

						brokerMonitorService = new BrokerMonitorService(ZookeeperClassLoader.getBrokerDaoImpl(), this, brokerId, session, sessionTimeout);
						brokerMonitorService.start();
					}
				}
			}
		}
	}

	public void removeSessionNodes(long session) {
		List<String> ephemeralPaths = zkDatabase.getEphemeralPaths(session);

		for (String ephemeralPath : ephemeralPaths) {
			try {
				delete(ephemeralPath, -1);
				logger.info(String.format("Invalidating session: ephemeral node deleted '%s'", ephemeralPath));

			} catch (KeeperException e) {
				logger.warn(String.format("Failed to remove session ephemeral node '%s'. This probably indicates that node was removed in meantime by different broker", ephemeralPath), e);
			}
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
				watchManager.registerChildWatch(path);
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
			watchManager.registerDataWatch(path);
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
					watchManager.registerDataWatch(path);
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
			if (stat != null) {
				watchManager.registerDataWatch( watcher, path);
			} else {
				watchManager.registerExistWatch(watcher, path);
			}
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
		if (event.getPath() != null) {
			remoteNodeUpdates.processLocalWatchedEvent(event);
		}
		watchesNotifier.processWatchedEvent(event);
	}
}
