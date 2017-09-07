package com.pega.zooikeeper.watches.service;

import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Manages watches subscriptions. Subscriptions are divided by three categories: data, child, exist.
 */
public class WatchServiceImpl implements WatchService {

	protected final Watcher defaultWatcher;
	private final Map<String, Set<Watcher>> dataWatches;
	private final Map<String, Set<Watcher>> existWatches;
	private final Map<String, Set<Watcher>> childWatches;
	private Logger logger = LoggerFactory.getLogger(WatchServiceImpl.class.getName());
	private boolean disableAutoWatchReset;

	public WatchServiceImpl(Watcher defaultWatcher, boolean disableAutoWatchReset) {
		this.disableAutoWatchReset = disableAutoWatchReset;
		this.dataWatches = new HashMap<String, Set<Watcher>>();
		this.existWatches =
				new HashMap<String, Set<Watcher>>();
		this.childWatches =
				new HashMap<String, Set<Watcher>>();
		this.defaultWatcher = defaultWatcher;
	}

	@Override
	public Watcher getDefaultWatcher() {
		return defaultWatcher;
	}

	@Override
	public void registerExistWatch(Watcher watcher, String path) {
		addWatch(watcher, path, getExistWatches());
	}

	@Override
	public void registerExistWatch(String path) {
		registerExistWatch(getDefaultWatcher(), path);
	}

	@Override
	public void registerDataWatch(Watcher watcher, String path) {
		addWatch(watcher, path, getDataWatches());
	}

	@Override
	public void registerDataWatch(String path) {
		registerDataWatch(getDefaultWatcher(), path);
	}

	@Override
	public void registerChildWatch(Watcher watcher, String path) {
		addWatch(watcher, path, getChildWatches());
	}

	@Override
	public void registerChildWatch(String path) {
		registerChildWatch(getDefaultWatcher(), path);
	}

	@Override
	public Set<Watcher> materialize(Watcher.Event.KeeperState state,
									Watcher.Event.EventType type,
									String clientPath) {
		Set<Watcher> result = new HashSet<Watcher>();

		switch (type) {
			case None:
				if(defaultWatcher != null ) {
					result.add(defaultWatcher);
				}
				boolean clear = disableAutoWatchReset && state != Watcher.Event.KeeperState.SyncConnected;
				synchronized (dataWatches) {
					for (Set<Watcher> ws : dataWatches.values()) {
						result.addAll(ws);
					}
					if (clear) {
						dataWatches.clear();
					}
				}

				synchronized (existWatches) {
					for (Set<Watcher> ws : existWatches.values()) {
						result.addAll(ws);
					}
					if (clear) {
						existWatches.clear();
					}
				}

				synchronized (childWatches) {
					for (Set<Watcher> ws : childWatches.values()) {
						result.addAll(ws);
					}
					if (clear) {
						childWatches.clear();
					}
				}

				return result;
			case NodeDataChanged:
			case NodeCreated:
				synchronized (dataWatches) {
					addTo(dataWatches.remove(clientPath), result);
				}
				synchronized (existWatches) {
					addTo(existWatches.remove(clientPath), result);
				}
				break;
			case NodeChildrenChanged:
				synchronized (childWatches) {
					addTo(childWatches.remove(clientPath), result);
				}
				break;
			case NodeDeleted:
				synchronized (dataWatches) {
					addTo(dataWatches.remove(clientPath), result);
				}
				// XXX This shouldn't be needed, but just in case
				synchronized (existWatches) {
					Set<Watcher> list = existWatches.remove(clientPath);
					if (list != null) {
						addTo(existWatches.remove(clientPath), result);
						logger.warn("We are triggering an exists watch for delete! Shouldn't happen!");
					}
				}
				synchronized (childWatches) {
					addTo(childWatches.remove(clientPath), result);
				}
				break;
			default:
				String msg = "Unhandled watch event type " + type
						+ " with state " + state + " on path " + clientPath;
				logger.error(msg);
				throw new RuntimeException(msg);
		}

		return result;
	}

	private void addTo(Set<Watcher> from, Set<Watcher> to) {
		if (from != null) {
			to.addAll(from);
		}
	}

	private Map<String, Set<Watcher>> getDataWatches() {
		return dataWatches;
	}

	private Map<String, Set<Watcher>> getExistWatches() {
		return existWatches;
	}

	private Map<String, Set<Watcher>> getChildWatches() {
		return childWatches;
	}

	private void addWatch(Watcher watcher, String clientPath, Map<String, Set<Watcher>> watches) {
		synchronized (watches) {
			Set<Watcher> watchers = watches.get(clientPath);
			if (watchers == null) {
				watchers = new HashSet<Watcher>();
				watches.put(clientPath, watchers);
			}
			watchers.add(watcher);
		}
	}
}
