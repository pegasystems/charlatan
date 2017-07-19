package org.apache.zookeeper.impl.node.service;

import org.apache.zookeeper.WatchedEvent;

/**
 * Created by natalia on 7/17/17.
 */
public interface NodeUpdateService {
	public void processNodeEvent(WatchedEvent event);
}
