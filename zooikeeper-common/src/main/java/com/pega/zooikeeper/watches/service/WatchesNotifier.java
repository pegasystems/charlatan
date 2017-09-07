package com.pega.zooikeeper.watches.service;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by natalia on 7/14/17.
 */
public class WatchesNotifier implements NodeUpdateListener, Runnable {

	private static Logger logger = LoggerFactory.getLogger(WatchesNotifier.class);

	private BlockingQueue<WatcherWatchedEvent> _blockingQueue = new LinkedBlockingDeque<WatcherWatchedEvent>();
	private WatchService watchService;
	public WatchesNotifier(WatchService watchService) {
		this.watchService = watchService;
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

	public void processWatchedEvent(WatchedEvent event, boolean sameThread) {
		Set<Watcher> watchers = watchService.materialize(event.getState(), event.getType(), event.getPath() );
		for(Watcher watcher : watchers) {
			if(sameThread)
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

