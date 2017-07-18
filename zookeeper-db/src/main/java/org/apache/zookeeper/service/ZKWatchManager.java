package org.apache.zookeeper.service;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.Code;
import org.apache.zookeeper.KeeperException.NoWatcherException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.WatcherType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Manages watches subscriptions. Subscriptions are divided by three categories: data, child, exist.
 *
 */
public class ZKWatchManager implements ClientWatchManager {

	protected final Watcher defaultWatcher;
	private final Map<String, Set<Watcher>> dataWatches;
	private final Map<String, Set<Watcher>> existWatches;
	private final Map<String, Set<Watcher>> childWatches;
	private Logger logger = LoggerFactory.getLogger(ZKWatchManager.class.getName());
	private boolean disableAutoWatchReset;

	public ZKWatchManager(Watcher defaultWatcher, boolean disableAutoWatchReset) {
		this.disableAutoWatchReset = disableAutoWatchReset;
		this.dataWatches = new HashMap<String, Set<Watcher>>();
		this.existWatches =
				new HashMap<String, Set<Watcher>>();
		this.childWatches =
				new HashMap<String, Set<Watcher>>();
		this.defaultWatcher = defaultWatcher;
	}

	final private void addTo(Set<Watcher> from, Set<Watcher> to) {
		if (from != null) {
			to.addAll(from);
		}
	}

	public Map<String, Set<Watcher>> getDataWatches() {
		return dataWatches;
	}

	public Map<String, Set<Watcher>> getExistWatches() {
		return existWatches;
	}

	public Map<String, Set<Watcher>> getChildWatches() {
		return childWatches;
	}

	public Watcher getDefaultWatcher() {
		return defaultWatcher;
	}

	public Map<EventType, Set<Watcher>> removeWatcher(String clientPath,
													  Watcher watcher, Watcher.WatcherType watcherType, boolean local, int rc)
			throws KeeperException {
		// Validate the provided znode path contains the given watcher of
		// watcherType
		containsWatcher(clientPath, watcher, watcherType);

		Map<EventType, Set<Watcher>> removedWatchers = new HashMap<EventType, Set<Watcher>>();
		HashSet<Watcher> childWatchersToRem = new HashSet<Watcher>();
		removedWatchers
				.put(EventType.ChildWatchRemoved, childWatchersToRem);
		HashSet<Watcher> dataWatchersToRem = new HashSet<Watcher>();
		removedWatchers.put(EventType.DataWatchRemoved, dataWatchersToRem);
		boolean removedWatcher = false;
		switch (watcherType) {
			case Children: {
				synchronized (childWatches) {
					removedWatcher = removeWatches(childWatches, watcher,
							clientPath, local, rc, childWatchersToRem);
				}
				break;
			}
			case Data: {
				synchronized (dataWatches) {
					removedWatcher = removeWatches(dataWatches, watcher,
							clientPath, local, rc, dataWatchersToRem);
				}

				synchronized (existWatches) {
					boolean removedDataWatcher = removeWatches(existWatches,
							watcher, clientPath, local, rc, dataWatchersToRem);
					removedWatcher |= removedDataWatcher;
				}
				break;
			}
			case Any: {
				synchronized (childWatches) {
					removedWatcher = removeWatches(childWatches, watcher,
							clientPath, local, rc, childWatchersToRem);
				}

				synchronized (dataWatches) {
					boolean removedDataWatcher = removeWatches(dataWatches,
							watcher, clientPath, local, rc, dataWatchersToRem);
					removedWatcher |= removedDataWatcher;
				}
				synchronized (existWatches) {
					boolean removedDataWatcher = removeWatches(existWatches,
							watcher, clientPath, local, rc, dataWatchersToRem);
					removedWatcher |= removedDataWatcher;
				}
			}
		}
		// Watcher function doesn't exists for the specified params
		if (!removedWatcher) {
			throw new KeeperException.NoWatcherException(clientPath);
		}
		return removedWatchers;
	}

	private boolean contains(String path, Watcher watcherObj,
							 Map<String, Set<Watcher>> pathVsWatchers) {
		boolean watcherExists = true;
		if (pathVsWatchers == null || pathVsWatchers.size() == 0) {
			watcherExists = false;
		} else {
			Set<Watcher> watchers = pathVsWatchers.get(path);
			if (watchers == null) {
				watcherExists = false;
			} else if (watcherObj == null) {
				watcherExists = watchers.size() > 0;
			} else {
				watcherExists = watchers.contains(watcherObj);
			}
		}
		return watcherExists;
	}

	/**
	 * Validate the provided znode path contains the given watcher and
	 * watcherType
	 *
	 * @param path        - client path
	 * @param watcher     - watcher object reference
	 * @param watcherType - type of the watcher
	 * @throws NoWatcherException
	 */
	void containsWatcher(String path, Watcher watcher,
						 WatcherType watcherType) throws NoWatcherException {
		boolean containsWatcher = false;
		switch (watcherType) {
			case Children: {
				synchronized (childWatches) {
					containsWatcher = contains(path, watcher, childWatches);
				}
				break;
			}
			case Data: {
				synchronized (dataWatches) {
					containsWatcher = contains(path, watcher, dataWatches);
				}

				synchronized (existWatches) {
					boolean contains_temp = contains(path, watcher,
							existWatches);
					containsWatcher |= contains_temp;
				}
				break;
			}
			case Any: {
				synchronized (childWatches) {
					containsWatcher = contains(path, watcher, childWatches);
				}

				synchronized (dataWatches) {
					boolean contains_temp = contains(path, watcher, dataWatches);
					containsWatcher |= contains_temp;
				}
				synchronized (existWatches) {
					boolean contains_temp = contains(path, watcher,
							existWatches);
					containsWatcher |= contains_temp;
				}
			}
		}
		// Watcher function doesn't exists for the specified params
		if (!containsWatcher) {
			throw new KeeperException.NoWatcherException(path);
		}
	}

	protected boolean removeWatches(Map<String, Set<Watcher>> pathVsWatcher,
									Watcher watcher, String path, boolean local, int rc,
									Set<Watcher> removedWatchers) throws KeeperException {
		if (!local && rc != Code.OK.intValue()) {
			throw KeeperException
					.create(KeeperException.Code.get(rc), path);
		}
		boolean success = false;
		// When local flag is true, remove watchers for the given path
		// irrespective of rc. Otherwise shouldn't remove watchers locally
		// when sees failure from server.
		if (rc == Code.OK.intValue() || (local && rc != Code.OK.intValue())) {
			// Remove all the watchers for the given path
			if (watcher == null) {
				Set<Watcher> pathWatchers = pathVsWatcher.remove(path);
				if (pathWatchers != null) {
					// found path watchers
					removedWatchers.addAll(pathWatchers);
					success = true;
				}
			} else {
				Set<Watcher> watchers = pathVsWatcher.get(path);
				if (watchers != null) {
					if (watchers.remove(watcher)) {
						// found path watcher
						removedWatchers.add(watcher);
						// cleanup <path vs watchlist>
						if (watchers.size() <= 0) {
							pathVsWatcher.remove(path);
						}
						success = true;
					}
				}
			}
		}
		return success;
	}

	/* (non-Javadoc)
	 * @see org.apache.zookeeper.ClientWatchManager#materialize(Event.KeeperState,
	 *                                                        Event.EventType, java.lang.String)
	 */
	@Override
	public Set<Watcher> materialize(Watcher.Event.KeeperState state,
									Watcher.Event.EventType type,
									String clientPath) {
		Set<Watcher> result = new HashSet<Watcher>();

		switch (type) {
			case None:
				result.add(defaultWatcher);
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
}