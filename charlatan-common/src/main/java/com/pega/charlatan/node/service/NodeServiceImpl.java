package com.pega.charlatan.node.service;

import com.pega.charlatan.node.bean.Node;
import com.pega.charlatan.node.dao.NodeDao;
import com.pega.charlatan.node.dao.RecordNotFoundException;
import com.pega.charlatan.watches.service.WatchService;
import com.pega.charlatan.node.bean.CreateMode;
import com.pega.charlatan.utils.CharlatanException;
import com.pega.charlatan.watches.bean.WatchedEvent;
import com.pega.charlatan.watches.bean.Watcher;
import com.pega.charlatan.node.bean.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by natalia on 7/19/17.
 */
public class NodeServiceImpl implements NodeService {
	private final NodeDao zkDatabase;
	private final WatchService watchService;

	private Logger logger = LoggerFactory.getLogger(NodeServiceImpl.class.getName());

	public NodeServiceImpl(NodeDao zkDatabase, WatchService watchService){

		this.zkDatabase = zkDatabase;
		this.watchService = watchService;

		watchService.start();
	}


	@Override
	public void close(long session) {
		removeEphemeralSessionNodes(session);
		processEvent(new WatchedEvent(Watcher.Event.Type.None, Watcher.Event.State.Disconnected, null));
	}

	@Override
	public String create(long session, String path, byte[] data, CreateMode createMode) throws CharlatanException {
		final Node node = new Node(path, data, createMode);

		try {
			if (!node.isRoot()) {
				Node parent = zkDatabase.get(node.getParentPath());
				int cversion = parent.getState().getCversion();

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
			node.getState().setCtime(now);
			node.getState().setMtime(now);

			if (zkDatabase.create(session, node)) {
				sendNewNodeEvents(node);
				return node.getPath();
			}

			throw new CharlatanException.NodeExistsException();
		} catch (RecordNotFoundException e) {
			throw new CharlatanException.NoNodeException(node.getParentPath());
		}
	}

	public void removeEphemeralSessionNodes(long session) {
		List<String> ephemeralPaths = zkDatabase.getEphemeralPaths(session);

		for (String ephemeralPath : ephemeralPaths) {
			try {
				delete(ephemeralPath, -1);
				logger.info(String.format("Invalidating session: ephemeral node deleted '%s'", ephemeralPath));

			} catch (CharlatanException e) {
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
		processEvent(new WatchedEvent(Watcher.Event.Type.NodeCreated, Watcher.Event.State.SyncConnected, node.getPath()));
		processEvent(new WatchedEvent(Watcher.Event.Type.NodeChildrenChanged, Watcher.Event.State.SyncConnected, node.getParentPath()));
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
	 * @throws CharlatanException
	 */
	@Override
	public void delete(String path, int version) throws CharlatanException {
		try {
			Node node = zkDatabase.get(path);

			if (version >= 0 && node.getState().getVersion() != version) {
				throw new CharlatanException.BadVersionException(path);
			}

			if (node.getChildren() != null && node.getChildren().size() > 0) {
				throw new CharlatanException.NotEmptyException(path);
			}

			if (zkDatabase.delete(node)) {

				processEvent(new WatchedEvent(Watcher.Event.Type.NodeDeleted, Watcher.Event.State.SyncConnected, path));
				processEvent(new WatchedEvent(Watcher.Event.Type.NodeChildrenChanged, Watcher.Event.State.SyncConnected, node.getParentPath()));
			}
			//node was modificated during the deletion process
			else {
				throw new CharlatanException.BadVersionException(path);
			}
		} catch (RecordNotFoundException r) {
			throw new CharlatanException.NoNodeException(path);
		}
	}

	@Override
	public Node getNode(String path, Watcher watcher, Watcher.Type watchType) throws CharlatanException {
		Node node = getNode(path);

		if (watcher != null) {
			watchService.registerWatch(watcher, watchType, path);
		}
		return node;
	}


	private Node getNode(String path) throws CharlatanException.NoNodeException {
		try {
			return zkDatabase.get(path);
		} catch (RecordNotFoundException e) {
			throw new CharlatanException.NoNodeException(path);
		}
	}

	@Override
	public NodeState setData(String path, byte[] data, int version) throws CharlatanException {
		Node node = getNode(path);

		if (version >= 0 && node.getState().getVersion() != version) {
			throw new CharlatanException.BadVersionException(path);
		}

		zkDatabase.update(path, data, version + 1, System.currentTimeMillis());

		node = getNode(path);

		processEvent(new WatchedEvent(Watcher.Event.Type.NodeDataChanged, Watcher.Event.State.SyncConnected, path));

		return node.getState();
	}

	@Override
	public NodeState exists(String path, Watcher watcher) {
		NodeState stat = null;
		try {
			Node node = zkDatabase.get(path);
			stat = node.getState();

		} catch (RecordNotFoundException e) {
		}

		if (watcher != null) {
			if (stat != null) {
				watchService.registerWatch(watcher, Watcher.Type.Data, path);
			} else {
				watchService.registerWatch(watcher, Watcher.Type.Exist, path);
			}
		}

		return stat;
	}

	@Override
	public void registerWatch(Watcher watcher, List<String> dataWatches, List<String> childWatches, List<String> existWatches) {
		if (dataWatches != null) {
			for (String dataWatch : childWatches) {
				watchService.registerWatch(watcher, Watcher.Type.Data, dataWatch);
			}
		}

		if (existWatches != null) {
			for (String childWatch : childWatches) {
				watchService.registerWatch(watcher, Watcher.Type.Children, childWatch);
			}
		}

		if (childWatches != null) {
			for (String existWatch : childWatches) {
				watchService.registerWatch(watcher, Watcher.Type.Exist, existWatch);
			}
		}

	}

	protected void processEvent(WatchedEvent event) {
		watchService.processLocalWatchedEvent(event);
	}
}
