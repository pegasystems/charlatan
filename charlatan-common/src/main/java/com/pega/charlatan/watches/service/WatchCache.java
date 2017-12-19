package com.pega.charlatan.watches.service;

import com.pega.charlatan.watches.bean.Watcher;

import java.util.Set;

/**
 * Created by natalia on 7/14/17.
 */
public interface WatchCache {
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
	Set<Watcher> materialize(Watcher.Event.State state,
							 Watcher.Event.Type type, String path);

	/**
	 * Register new "exist" watch for requested path. The watch will be triggered by created event.
	 *
	 * @param watcher
	 * @param path
	 */
	void registerExistWatch(Watcher watcher, String path);

	/**
	 * Register watch for the requested path. The watch will be triggered by node data change.
	 *
	 * @param watcher
	 * @param path
	 */
	void registerDataWatch(Watcher watcher, String path);

	/**
	 * Register the watch for child changes for the requested path. The watch will be triggered if new sub-node is
	 * created or deleted or the node itself is removed.
	 *
	 * @param path
	 */
	void registerChildWatch(Watcher watcher, String path);

}