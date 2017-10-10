package com.pega.charlatan.watches.service;

import org.apache.zookeeper.WatchedEvent;

/**
 * Created by natalia on 7/17/17.
 */
public interface WatchedEventListener {
	void processWatchedEvent(WatchedEvent event, boolean blocking);
}
