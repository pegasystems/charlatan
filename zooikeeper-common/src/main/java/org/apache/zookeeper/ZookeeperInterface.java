package org.apache.zookeeper;

import com.pega.zooikeeper.node.service.NodeService;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;

public interface ZookeeperInterface extends NodeService {

	String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException;

	void create(final String path, final byte[] data, final List<ACL> acl, final CreateMode createMode, final AsyncCallback.StringCallback cb, final Object ctx);

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
}
