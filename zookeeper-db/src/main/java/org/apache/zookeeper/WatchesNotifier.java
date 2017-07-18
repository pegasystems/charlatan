package org.apache.zookeeper;

import org.apache.zookeeper.service.ClientWatchManager;
import org.apache.zookeeper.service.NodeUpdateService;

import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;

/**
 * Created by natalia on 7/14/17.
 */
public class WatchesNotifier implements Runnable {
	private BlockingQueue<WatcherWatchedEvent> _blockingQueue = new LinkedBlockingDeque<WatcherWatchedEvent>();
	private ClientWatchManager watchManager;
	private NodeUpdateService nodeUpdateService;

	public WatchesNotifier(ClientWatchManager watchManager) {
		this.watchManager = watchManager;
	}

	@Override
	public void run() {
		try {
			while (true) {
				WatcherWatchedEvent we = _blockingQueue.take();
				we.getWatcher().process(we.getWatchedEvent());
			}
		} catch (InterruptedException e) {
			// stop event thread
		}
	}

	public void send(WatchedEvent event) {
		Set<Watcher> watchers = watchManager.materialize(event.getState(), event.getType(), event.getPath() );
		for(Watcher watcher : watchers) {
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

