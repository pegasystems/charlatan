package com.pega.charlatan.watches.dao;

import com.pega.charlatan.node.bean.NodeUpdate;

import java.util.List;

/**
 * Created by natalia on 7/17/17.
 */
public interface NodeUpdateDao {

	void insertUpdate(NodeUpdate update);

	List<NodeUpdate> getNodeUpdates(int thisBroker, long fromTimestamp);

	void clearOldUpdates(long toMs);
}
