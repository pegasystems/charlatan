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
	 * If node already exists - exception is thrown.
	 *
	 * @return
	 */
	boolean create(Node node) throws RecordNotFoundException;

	/**
	 * Delete node
	 *
	 * @return
	 */
	boolean delete(Node node);

	void deleteEphemeralNodes(long session);

	Node get(String path) throws RecordNotFoundException;

	void update(Node node) throws RecordNotFoundException;

	/**
	 * Returns list of the children node names. Child node name is a relative name to the parent node.
	 * @param parent
	 * @return
	 * @throws RecordNotFoundException
	 */
	List<String> getChildren(Node parent) throws RecordNotFoundException;
}
