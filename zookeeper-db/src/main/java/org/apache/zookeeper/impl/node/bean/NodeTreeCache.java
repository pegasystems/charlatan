package org.apache.zookeeper.impl.node.bean;

import com.google.common.collect.ImmutableList;

import java.util.*;

/**
 * Node cache of the current session.
 */
public class NodeTreeCache {

	/**
	 * Maps node path to node
	 */
	private Map<String, Node> nodes = new HashMap();
	private List<String> sessionEphemeralPaths = new LinkedList<>();

	public void setPathNode(String path, Node node) {
		nodes.put(path, node);
	}

	public void addSessionEphemeralPath(String path){
		sessionEphemeralPaths.add(path);
	}

	/**
	 * Mark node as removed: path references to null.
	 *
	 * @param path
	 */
	public void removeNode(String path) {
		nodes.put(path,null);
		sessionEphemeralPaths.remove(path);

	}

	/**
	 * True if the requested path is known. Note that un-existing node path is known if corresponded node is null.
	 * @param path
	 * @return
	 */
	public boolean containsPath( String path ) {
		return nodes.containsKey(path);
	}

	/**
	 * Returns node that corresponds to the requested path.
	 * Returns null if path wasn't cached.
	 * Returns null if path is known as un-existed.
	 * @param path
	 * @return
	 */
	public Node get(String path){
		return nodes.get(path);
	}

	/**
	 * Get all node paths that were created as 'ephemeral' during the current session.
	 * @return
	 */
	public List<String> getSessionEphemeralPaths() {
		return ImmutableList.copyOf(sessionEphemeralPaths);
	}

	/**
	 * Removes information about the requested path from the cache.
	 * @param path
	 */
	public void invalidate(String path){
		nodes.remove(path);
	}
}
