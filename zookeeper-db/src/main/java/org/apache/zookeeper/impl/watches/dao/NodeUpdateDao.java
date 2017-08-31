package org.apache.zookeeper.impl.watches.dao;

import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.impl.node.bean.NodeUpdate;

import java.util.List;

/**
 * Created by natalia on 7/17/17.
 */
public interface NodeUpdateDao {

	void insertUpdate(NodeUpdate update);

	List<NodeUpdate> getNodeUpdates(int thisBroker, long fromTimestamp);

	void clearOldUpdates(long toMs);
}
