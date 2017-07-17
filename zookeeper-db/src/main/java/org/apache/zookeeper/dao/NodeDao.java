package org.apache.zookeeper.dao;

import org.apache.zookeeper.dao.bean.Node;

import java.util.List;

/**
 * Created by natalia on 7/11/17.
 */
public interface NodeDao {

	/**
	 * True if requested path exists
	 *
	 * @return
	 */
	boolean exists(Node node);

	/**
	 * Stores node and node data.
	 *
	 * @throws RecordNotFoundException In case parent node doesn't exist
	 * @return Returns true if node was created. Returns false if node with the given path already existed.
	 */
	boolean create(Node node) throws RecordNotFoundException;

	/**
	 * Deletes node
	 *
	 * @return
	 */
	boolean delete(Node node);

	/**
	 * Deletes ephemeral node that was created by specific session.
	 *
	 * @param session
	 */
	void deleteEphemeralNodes(long session);

	/**
	 * Returns node that corresponds to requested path
	 * @param path
	 * @return
	 * @throws RecordNotFoundException
	 */
	Node get(String path) throws RecordNotFoundException;

	/**
	 * Updates node
	 * @param node
	 */
	void update(Node node);

	/**
	 * Returns list of the children node names. Child node name is a relative name to the parent node.
	 * @param parent
	 * @return
	 * @throws RecordNotFoundException
	 */
	List<String> getChildren(Node parent) throws RecordNotFoundException;
}
