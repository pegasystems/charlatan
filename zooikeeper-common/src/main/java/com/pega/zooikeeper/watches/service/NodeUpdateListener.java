package com.pega.zooikeeper.watches.service;

import org.apache.zookeeper.WatchedEvent;

/**
 * Created by natalia on 7/17/17.
 */
public interface NodeUpdateListener {
	void processWatchedEvent(WatchedEvent event, boolean blocking);
}
