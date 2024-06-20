package com.pega.charlatan.watches.service;

import com.pega.charlatan.watches.bean.WatchedEvent;
import com.pega.charlatan.watches.bean.Watcher;

import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by natalia on 7/20/17.
 */
public abstract class WatchService {

	private CopyOnWriteArrayList<WatchedEventListener> nodeUpdateListeners;
	private WatchesNotifier watchesNotifier;
	private WatchCache watchCache;

	public WatchService(WatchCache watchCache) {
		this.watchCache = watchCache;

		this.watchesNotifier = new WatchesNotifier(watchCache);
		nodeUpdateListeners = new CopyOnWriteArrayList<>();
	}

	public void addNodeUpdateListener(WatchedEventListener l) {
		nodeUpdateListeners.add(l);
	}

	public void removeNodeUpdateListener(WatchedEventListener l) {
		nodeUpdateListeners.remove(l);
	}

	/**
	 * WatchedEvent was triggered by remote server/broker. WatchedEvent will be processed by all subscribers.
	 *
	 * @param event
	 */
	public void processRemoteWatchedEvent(WatchedEvent event) {
		watchesNotifier.processWatchedEvent(event, false);

		// notify listeners
		for (WatchedEventListener l : nodeUpdateListeners) {
			l.processWatchedEvent(event, false);
		}
	}

	/**
	 * WatchedEvent was triggered locally.
	 * The implementation of this method should ensure that the event will become available to all servers/brokers except the current one.
	 *
	 * @param event
	 */
	public final void processLocalWatchedEvent(WatchedEvent event) {
		watchesNotifier.processWatchedEvent(event, false);

		//Only emitter session is interested in None event
		if (event.getType() != Watcher.Event.Type.None) {
			communicateEvent(event);
		}
	}

	/**
	 * Communicate the event to everyone interested
	 */
	protected abstract void communicateEvent(WatchedEvent event);

	public void start() {
		ExecutorService executor = Executors.newSingleThreadExecutor();
		executor.submit(watchesNotifier);
	}

	;

	public void registerWatch(Watcher watcher, Watcher.Type type, String path) {
		switch (type) {
			case Children:
				watchCache.registerChildWatch(watcher, path);
				break;
			case Data:
				watchCache.registerDataWatch(watcher, path);
				break;
			case Exist:
				watchCache.registerExistWatch(watcher, path);
				break;
		}
	}
}
