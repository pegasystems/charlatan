package org.apache.zookeeper.impl.node.service;

import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

/**
 * Created by natalia on 7/19/17.
 */
public interface NodeService {

	void close();

	/**
	 * Create a node with the given path and data.
	 * If a node with the same actual path already exists in the ZooKeeper, a
	 * KeeperException with error code KeeperException.NodeExists will be
	 * thrown.
	 * <p>
	 * If the parent node does not exist in the ZooKeeper, a KeeperException
	 * with error code KeeperException.NoNode will be thrown.
	 *
	 * @param path
	 * @param data
	 * @param acl
	 * @param createMode
	 * @return
	 */
	String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException;

	void create(final String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctx);

	/**
	 * Deletes the node with specified path and version.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown
	 * if the nodes does not exist.
	 * <p>
	 * A KeeperException with error code KeeperException.BadVersion will be
	 * thrown if the given version does not match the node's version.
	 * <p>
	 * A KeeperException with error code KeeperException.NotEmpty will be thrown
	 * if the node has children.
	 *
	 * @param path
	 * @param version
	 * @throws KeeperException
	 */
	void delete(String path, int version) throws KeeperException;

	/**
	 * Returns the list of the children of the node of the given path.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown
	 * if no node with the given path exists.
	 *
	 * @param path
	 * @param watch
	 * @return
	 * @throws KeeperException
	 */
	List<String> getChildren(String path, boolean watch) throws KeeperException;

	/**
	 * Return the data and the stat of the node of the given path.
	 * <p>
	 * A KeeperException with error code KeeperException.NoNode will be thrown
	 * if no node with the given path exists.
	 *
	 * @param path
	 * @param watch
	 * @param stat
	 * @return
	 * @throws KeeperException
	 */
	byte[] getData(String path, boolean watch, Stat stat) throws KeeperException;

	void getData(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx);

	/**
	 * Set the data for the node of the given path if such a node exists and the
	 * given version matches the version of the node (if the given version is
	 * -1, it matches any node's versions).
	 * <p>
	 *
	 * @param path
	 * @param data
	 * @param version
	 * @return The stat of the node
	 * @throws KeeperException
	 */
	Stat setData(String path, byte[] data, int version) throws KeeperException;

	/**
	 * Returns the stat of the node of the given path. Return null if no such a
	 * node exists.
	 *
	 * @param path
	 * @param watch
	 * @return
	 */
	Stat exists(String path, boolean watch);

	Stat exists(String path, Watcher watcher);

	void setACL(String path, List<ACL> acl, int version, AsyncCallback.StatCallback cb, Object ctx);

	/**
	 * The asynchronous version of getChildren. Return the list of the children of the node of the given path.
	 *
	 * @param path
	 * @param watch
	 * @param cb
	 * @param ctx
	 */
	void getChildren(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx);

	/**
	 * The session id for this ZooKeeper client instance.
	 *
	 * @return
	 */
	long getSessionId();

}
