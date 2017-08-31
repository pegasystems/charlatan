package org.apache.zookeeper.impl.watches.service;

import org.apache.zookeeper.WatchedEvent;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by natalia on 7/20/17.
 */
public abstract class RemoteNodeUpdateManager {

	private CopyOnWriteArrayList<NodeUpdateListener> nodeUpdateListeners;

	public RemoteNodeUpdateManager() {
		nodeUpdateListeners = new CopyOnWriteArrayList<>();
	}

	public void addNodeUpdateListener(NodeUpdateListener l) {
		nodeUpdateListeners.add(l);
	}

	public void removeNodeUpdateListener(NodeUpdateListener l) {
		nodeUpdateListeners.remove(l);
	}

	/**
	 * WatchedEvent was triggered by remote broker. WatchedEvent will be processed by all subscribers.
	 *
	 * @param event
	 */
	public void processRemoteWatchedEvent(WatchedEvent event) {
		for (NodeUpdateListener l : nodeUpdateListeners) {
			l.processWatchedEvent(event);
		}
	}

	/**
	 * WatchedEvent was triggered locally.
	 * The implementation of this method should ensure that the event will become available to all brokers except the current one.
	 *
	 * @param event
	 */
	public abstract void processLocalWatchedEvent(WatchedEvent event);

	public abstract void start();
}
