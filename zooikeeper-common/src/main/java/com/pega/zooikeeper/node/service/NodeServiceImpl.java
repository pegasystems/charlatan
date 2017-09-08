package com.pega.zooikeeper.node.service;

import com.pega.zooikeeper.node.bean.Node;
import com.pega.zooikeeper.node.dao.NodeDao;
import com.pega.zooikeeper.node.dao.RecordNotFoundException;
import com.pega.zooikeeper.watches.service.WatchCache;
import com.pega.zooikeeper.watches.service.WatchService;
import com.pega.zooikeeper.watches.service.WatchesNotifier;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by natalia on 7/19/17.
 */
public class NodeServiceImpl implements NodeService {

//	protected final WatchesNotifier watchesNotifier;
	private final NodeDao zkDatabase;
	private final WatchService watchService;
//	private final WatchCache watchCache;

	private Logger logger = LoggerFactory.getLogger(NodeServiceImpl.class.getName());

	public NodeServiceImpl(NodeDao zkDatabase, WatchService watchService){

		this.zkDatabase = zkDatabase;
		this.watchService = watchService;

		watchService.start();
	}


	@Override
	public void close(long session) {
		removeEphemeralSessionNodes(session);
		processEvent(new WatchedEvent(Watcher.Event.EventType.None, Watcher.Event.KeeperState.Disconnected, null));
	}

	@Override
	public String create(long session, String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException {
		final Node node = new Node(path, data, createMode);

		try {
			if (!node.isRoot()) {
				Node parent = zkDatabase.get(node.getParentPath());
				int cversion = parent.getStat().getCversion();

				if (createMode.isSequential()) {
					// The number of changes to the children of this znode.
					path = path + String.format("%010d", cversion);
					node.setPath(path);
				}

				// Small optimization: sequential branches are never created in the root
				if (!parent.isRoot()) {
					zkDatabase.updateCVersion(parent.getPath(), cversion + 1);
				}
			}

			long now = System.currentTimeMillis();
			node.getStat().setCtime(now);
			node.getStat().setMtime(now);

			if (zkDatabase.create(session, node)) {
				sendNewNodeEvents(node);
				return node.getPath();
			}

			throw new KeeperException.NodeExistsException();
		} catch (RecordNotFoundException e) {
			throw new KeeperException.NoNodeException(node.getParentPath());
		}
	}

	public void removeEphemeralSessionNodes(long session) {
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
	public List<String> getChildren(String path, Watcher watcher) throws KeeperException {
		try {

			Node node = zkDatabase.get(path);

			if (watcher != null) {
				watchService.registerWatch(watcher, Watcher.WatcherType.Children, path);
			}

			return node.getChildren();
		} catch (RecordNotFoundException e) {
			throw new KeeperException.NoNodeException(path);
		}
	}

	@Override
	public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException {
		Node node = getNode(path);

		if (stat != null) {
			stat.loadFrom(node.getStat());
		}

		if (watcher != null) {
			watchService.registerWatch(watcher, Watcher.WatcherType.Data, path);
		}
		return node.getData();
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
				watchService.registerWatch(watcher, Watcher.WatcherType.Data, path);
			} else {
				watchService.registerWatch(watcher, Watcher.WatcherType.Exist, path);
			}
		}

		return stat;
	}

	@Override
	public void registerWatch(Watcher watcher, List<String> dataWatches, List<String> childWatches, List<String> existWatches) {
		if (dataWatches != null) {
			for (String dataWatch : childWatches) {
				watchService.registerWatch(watcher, Watcher.WatcherType.Data, dataWatch);
			}
		}

		if (existWatches != null) {
			for (String childWatch : childWatches) {
				watchService.registerWatch(watcher, Watcher.WatcherType.Children, childWatch);
			}
		}

		if (childWatches != null) {
			for (String existWatch : childWatches) {
				watchService.registerWatch(watcher, Watcher.WatcherType.Exist, existWatch);
			}
		}

	}

	protected void processEvent(WatchedEvent event) {
		watchService.processLocalWatchedEvent(event);
	}
}
