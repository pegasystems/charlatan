package com.pega.zooikeeper.watches.dao;

import com.pega.zooikeeper.node.bean.NodeUpdate;
import org.apache.zookeeper.data.Id;

import java.util.List;

/**
 * Created by natalia on 7/17/17.
 */
public interface NodeUpdateDao {

	void insertUpdate(NodeUpdate update);

	List<NodeUpdate> getNodeUpdates(int thisBroker, long fromTimestamp);

	void clearOldUpdates(long toMs);
}
