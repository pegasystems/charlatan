package org.apache.zookeeper.impl.watches.service;

import org.apache.zookeeper.Watcher;

import java.util.Set;

/**
 * Created by natalia on 7/14/17.
 */
public interface ClientWatchManager {
	/**
	 * Return a set of watchers that should be notified of the event. The
	 * manager must not notify the watcher(s), however it will update it's
	 * internal structure as if the watches had triggered. The intent being
	 * that the callee is now responsible for notifying the watchers of the
	 * event, possibly at some later time.
	 *
	 * @param state event state
	 * @param type  event type
	 * @param path  event path
	 * @return may be empty set but must not be null
	 */
	Set<Watcher> materialize(Watcher.Event.KeeperState state,
							 Watcher.Event.EventType type, String path);

	/**
	 * Returns the default watcher
	 *
	 * @return
	 */
	Watcher getDefaultWatcher();

	/**
	 * Register new "exist" watch for requested path. The watch will be triggered by created event.
	 *
	 * @param watcher
	 * @param path
	 */
	void registerExistWatch(Watcher watcher, String path);

	/**
	 * Register default watch with the requested path. The watch will be triggered by created event.
	 *
	 * @param path
	 */
	void registerExistWatch(String path);

	/**
	 * Register watch for the requested path. The watch will be triggered by node data change.
	 * @param watcher
	 * @param path
	 */
	void registerDataWatch(Watcher watcher, String path);

	/**
	 * Register default watch for the requested path. The watch will be triggered by node data change
	 *
	 * @param path
	 */
	void registerDataWatch(String path);

	/**
	 * Register default watch for child changes for the requested path. The watch will be triggered if new sub-node is
	 * created or deleted or the node itself is removed.
	 *
	 * @param path
	 */
	void registerChildWatch(String path);
}