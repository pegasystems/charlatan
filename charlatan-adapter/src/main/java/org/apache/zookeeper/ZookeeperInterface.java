package org.apache.zookeeper;

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public interface ZookeeperInterface {

	String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException;

	void create(final String path, final byte[] data, final List<ACL> acl, final CreateMode createMode, final AsyncCallback.StringCallback cb, final Object ctx);

	/**
	 * Create a node with the given path and data.
	 * <p>
	 * The createMode argument can also specify to create a sequential node. The
	 * actual path name of a sequential node will be the given path plus a
	 * suffix "i" where i is the current sequential number of the node. The sequence
	 * number is always fixed length of 10 digits, 0 padded. Once
	 * such a node is created, the sequential number will be incremented by one.
	 * <p>
	 * If a node with the same actual path already exists in the ZooKeeper, a
	 * KeeperException with error code KeeperException.NodeExists will be
	 * thrown.
	 * <p>
	 * An ephemeral node cannot have children. If the parent node of the given
	 * path is ephemeral, a KeeperException with error code
	 * KeeperException.NoChildrenForEphemerals will be thrown.
	 * <p>
	 * If the parent node does not exist in the ZooKeeper, a KeeperException
	 * with error code KeeperException.NoNode will be thrown.
	 * <p>
	 * This operation, if successful, will trigger all the watches left on the
	 * node of the given path by exists and getData API calls, and the watches
	 * left on the parent node by getChildren API calls.
	 * <p>
	 * If a node is created successfully, the ZooKeeper server will trigger the
	 * watches on the path left by exists calls, and the watches on the parent
	 * of the node by getChildren calls.
	 *
	 * @param path       node path
	 * @param data       node data
	 * @param acl        node acl
	 * @param createMode node mode
	 * @return the actual path of the created node
	 */
	String create(long session, String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException;

	/**
	 * The asynchronous version of getChildren. Return the list of the children of the node of the given path.
	 *
	 * @param path
	 * @param watch
	 * @param cb
	 * @param ctx
	 */
	void getChildren(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx);

	List<String> getChildren(String path, boolean watch) throws KeeperException;

	/**
	 * The asynchronous version of getData.
	 */
	void getData(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx);

	byte[] getData(String path, boolean watch, Stat stat) throws KeeperException;

	Stat exists(String path, boolean watch);

	void setACL(String path, List<ACL> acl, int version, AsyncCallback.StatCallback cb, Object ctx);

	public long getSessionId();

	void close(long session);

	/**
	 * Delete the node with the given path. The call will succeed if such a node
	 * exists, and the given version matches the node's version (if the given
	 * version is -1, it matches any node's versions).
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown
	 * if the nodes does not exist.
	 * <p>
	 * A KeeperException with error code KeeperException.BadVersion will be
	 * thrown if the given version does not match the node's version.
	 * <p>
	 * A KeeperException with error code KeeperException.NotEmpty will be thrown
	 * if the node has children.
	 * <p>
	 * This operation, if successful, will trigger all the watches on the node
	 * of the given path left by exists API calls, and the watches on the parent
	 * node left by getChildren API calls.
	 *
	 * @param path    the path of the node to be deleted.
	 * @param version the expected node version.
	 * @throws KeeperException If the server signals an error with a non-zero
	 *                         return code.
	 */
	void delete(String path, int version) throws KeeperException;

	/**
	 * Return the list of the children of the node of the given path.
	 * <p>
	 * If the watcher isn't null and the call is successful (no exception is thrown),
	 * a watch will be left on the node with the given path. The watch will be
	 * triggered by a successful operation that deletes the node of the given
	 * path or creates/delete a child under the node.
	 * <p>
	 * The list of children returned is not sorted and no guarantee is provided
	 * as to its natural or lexical order.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown
	 * if no node with the given path exists.
	 *
	 * @param path
	 * @param watcher
	 * @return an unordered array of children of the node with the given path
	 * @throws KeeperException If the server signals an error with a non-zero error code.
	 */
	List<String> getChildren(String path, Watcher watcher) throws KeeperException;

	/**
	 * Return the data and the stat of the node of the given path.
	 * <p>
	 * If the watch is true and the call is successful (no exception is
	 * thrown), a watch will be left on the node with the given path. The watch
	 * will be triggered by a successful operation that sets data on the node, or
	 * deletes the node.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown
	 * if no node with the given path exists.
	 *
	 * @param path  the given path
	 * @param watch whether need to watch this node
	 * @param stat  the stat of the node
	 * @return the data of the node
	 * @throws KeeperException If the server signals an error with a non-zero error code
	 */
	byte[] getData(String path, Watcher watch, Stat stat) throws KeeperException;

	/**
	 * Set the data for the node of the given path if such a node exists and the
	 * given version matches the version of the node (if the given version is
	 * -1, it matches any node's versions). Return the stat of the node.
	 * <p>
	 * This operation, if successful, will trigger all the watches on the node
	 * of the given path left by getData calls.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown
	 * if no node with the given path exists.
	 * <p>
	 * A KeeperException with error code KeeperException.BadVersion will be
	 * thrown if the given version does not match the node's version.
	 *
	 * @param path    the path of the node
	 * @param data    the data to set
	 * @param version the expected matching version
	 * @return the state of the node
	 * @throws KeeperException If the server signals an error with a non-zero error code.
	 */
	Stat setData(String path, byte[] data, int version) throws KeeperException;

	/**
	 * Return the stat of the node of the given path. Return null if no such a
	 * node exists.
	 * <p>
	 * If the watcher is set and the call is successful (no exception is thrown),
	 * a watch will be left on the node with the given path. The watch will be
	 * triggered by a successful operation that creates/delete the node or sets
	 * the data on the node.
	 *
	 * @param path  the node path
	 * @param watcher whether need to watch this node
	 * @return the stat of the node of the given path; return null if no such a
	 * node exists.
	 */
	Stat exists(String path, Watcher watcher);

	void removeEphemeralSessionNodes(long session);

	void registerWatch(Watcher watcher, List<String> dataWatches, List<String> childWatches, List<String> existWatches);
}
