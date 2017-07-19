package org.apache.zookeeper.impl.node.service;

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
	 * @param type event type
	 * @param path event path
	 * @return may be empty set but must not be null
	 */
	public Set<Watcher> materialize(Watcher.Event.KeeperState state,
									Watcher.Event.EventType type, String path);
}