package com.pega.charlatan.watches.service;


import com.pega.charlatan.watches.bean.WatchedEvent;
import com.pega.charlatan.watches.bean.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Notifies all watches interested in specific WatchedEvent
 */
public class WatchesNotifier implements WatchedEventListener, Runnable {

	private static Logger logger = LoggerFactory.getLogger(WatchesNotifier.class);

	private BlockingQueue<WatcherWatchedEvent> _blockingQueue = new LinkedBlockingDeque<WatcherWatchedEvent>();
	private WatchCache watchCache;
	public WatchesNotifier(WatchCache watchCache) {
		this.watchCache = watchCache;
	}

	@Override
	public void run() {
		try {
			while (true) {
				WatcherWatchedEvent we = _blockingQueue.take();
				we.getWatcher().process(we.getWatchedEvent());

				logger.debug("Send watched event " + we.getWatchedEvent().toString() );
			}
		} catch (InterruptedException e) {
			// stop event thread
		}
	}

	public void processWatchedEvent(WatchedEvent event, boolean blocking) {
		Set<Watcher> watchers = watchCache.materialize(event.getState(), event.getType(), event.getPath() );
		for(Watcher watcher : watchers) {
			if(blocking)
				watcher.process(event);
			else
				_blockingQueue.add(new WatcherWatchedEvent(watcher, event));
		}
	}

	class WatcherWatchedEvent {
		private Watcher watcher;
		private WatchedEvent event;

		public WatcherWatchedEvent(Watcher watcher, WatchedEvent event) {
			this.watcher = watcher;
			this.event = event;
		}

		public Watcher getWatcher() {
			return watcher;
		}

		public WatchedEvent getWatchedEvent() {
			return event;
		}
	}
}

