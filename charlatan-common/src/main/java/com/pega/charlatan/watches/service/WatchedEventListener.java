package com.pega.charlatan.watches.service;


import com.pega.charlatan.watches.bean.WatchedEvent;

/**
 * Created by natalia on 7/17/17.
 */
public interface WatchedEventListener {
	void processWatchedEvent(WatchedEvent event, boolean blocking);
}
