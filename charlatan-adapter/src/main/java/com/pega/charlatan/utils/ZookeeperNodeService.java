package com.pega.charlatan.utils;



import com.pega.charlatan.node.bean.Node;
import com.pega.charlatan.node.service.NodeService;
import com.pega.charlatan.node.bean.NodeState;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.KeeperException;

import java.util.List;

/**
 * NodeService wrapper that translates Zookeeper API to the Charlatan API
 */
public class ZookeeperNodeService {

	private final NodeService nodeService;

	public ZookeeperNodeService(NodeService nodeService) {
		this.nodeService = nodeService;
	}

	public void close(long session) {
		nodeService.close(session);
	}

	public String create(long session, String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException {
		try {
			return nodeService.create(session, path, data, com.pega.charlatan.node.bean.CreateMode.fromFlag(createMode.toFlag()));
		} catch (CharlatanException e) {
			throw toKeeperException(e);
		}
	}

	public void delete(String path, int version) throws KeeperException {
		try {
			nodeService.delete(path, version);
		} catch (CharlatanException e) {
			throw toKeeperException(e);
		}
	}

	public List<String> getChildren(String path, Watcher watcher) throws KeeperException {
		try {
			Node node = nodeService.getNode(path, toCharlatanWatcher(watcher), com.pega.charlatan.watches.bean.Watcher.Type.Children);
			return node.getChildren();
		} catch (CharlatanException e) {
			throw toKeeperException(e);
		}
	}

	public byte[] getData(String path, Watcher watcher, Stat stat) throws KeeperException {
		try {
			Node node = nodeService.getNode(path, toCharlatanWatcher(watcher), com.pega.charlatan.watches.bean.Watcher.Type.Data);
			loadStat(stat, node.getState());
			return node.getData();
		} catch (CharlatanException e) {
			throw toKeeperException(e);
		}
	}

	public Stat setData(String path, byte[] data, int version) throws KeeperException {
		try {
			NodeState nodeState =  nodeService.setData(path,data, version);
			return toStat(nodeState);
		} catch (CharlatanException e) {
			throw toKeeperException(e);
		}
	}

	public Stat exists(String path, Watcher watcher) {
		NodeState nodeState = nodeService.exists(path, toCharlatanWatcher(watcher));
		return toStat(nodeState);
	}

	public void removeEphemeralSessionNodes(long session) {
		nodeService.removeEphemeralSessionNodes(session);
	}

	public void registerWatch(Watcher watcher, List<String> dataWatches, List<String> childWatches, List<String> existWatches) {
		nodeService.registerWatch(toCharlatanWatcher(watcher), dataWatches, childWatches, existWatches);
	}


	private Stat toStat(NodeState nodeState) {
		Stat stat = new Stat();
		loadStat(stat, nodeState);

		return stat;
	}

	private void loadStat(Stat stat, NodeState nodeState) {
		stat.setVersion(nodeState.getVersion());
		stat.setAversion(nodeState.getAversion());
		stat.setCversion(nodeState.getCversion());
		stat.setCtime(nodeState.getCtime());
		stat.setMtime(nodeState.getMtime());
		stat.setCzxid(nodeState.getCzxid());
		stat.setMzxid(nodeState.getMzxid());
		stat.setPzxid(nodeState.getPzxid());
		stat.setDataLength(nodeState.getDataLength());
		stat.setEphemeralOwner(nodeState.getEphemeralOwner());
		stat.setNumChildren(nodeState.getNumChildren());
	}

	private com.pega.charlatan.watches.bean.Watcher toCharlatanWatcher(final Watcher watcher) {
		return new com.pega.charlatan.watches.bean.Watcher() {
			@Override
			public void process(com.pega.charlatan.watches.bean.WatchedEvent event) {
				watcher.process(toWatchedEvent(event));
			}
		};
	}

	private WatchedEvent toWatchedEvent(com.pega.charlatan.watches.bean.WatchedEvent charlatanWatchedEvent) {
		return new WatchedEvent(Watcher.Event.EventType.fromInt(charlatanWatchedEvent.getType().getCode()),
				Watcher.Event.KeeperState.fromInt(charlatanWatchedEvent.getState().getCode()),
				charlatanWatchedEvent.getPath());
	}

	private KeeperException toKeeperException(CharlatanException e) {
		return KeeperException.create(e.code().intValue(), e.getPath());
	}
}
