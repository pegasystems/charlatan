package org.apache.zookeeper.dao;

import org.apache.zookeeper.bean.NodeUpdate;

import java.util.List;

/**
 * Created by natalia on 7/17/17.
 */
public interface NodeUpdateDao {

	void insertUpdate(int ownerBroker, NodeUpdate update);

	List<NodeUpdate> getNodeUpdates(int ownerBroker, int fromId);

	void clearProcessedUpdates(int ownerBroker, int id);

	void clearOldUpdates(long toMs);

	int getLastUpdateId();
}
