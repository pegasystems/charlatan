package org.apache.zookeeper.impl.watches.dao;

import org.apache.zookeeper.impl.node.bean.NodeUpdate;

import java.util.List;

/**
 * Created by natalia on 7/17/17.
 */
public interface NodeUpdateDao {

	void insertUpdate(NodeUpdate update);

	List<NodeUpdate> getNodeUpdates(int ownerBroker, int fromId);

	void clearProcessedUpdates(int ownerBroker, int id);

	void clearOldUpdates(long toMs);

	int getLastUpdateId();
}
