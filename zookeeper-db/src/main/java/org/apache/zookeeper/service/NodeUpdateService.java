package org.apache.zookeeper.service;

import org.apache.zookeeper.WatchedEvent;

/**
 * Created by natalia on 7/17/17.
 */
public interface NodeUpdateService {
	public void processNodeEvent(WatchedEvent event);
}
