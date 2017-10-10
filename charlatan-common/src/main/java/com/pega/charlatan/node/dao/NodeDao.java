package com.pega.charlatan.node.dao;


import com.pega.charlatan.node.bean.Node;

import java.util.List;

/**
 * Created by natalia on 7/11/17.
 */
public interface NodeDao {

//NodeDao	/**
//	 * True if requested path exists
//	 *
//	 * @return
//	 */
//	boolean exists(Node node);

	/**
	 * Stores node and node data.
	 *
	 * @return Returns true if node was created. Returns false if node with the given path already existed.
	 * @throws RecordNotFoundException In case parent node doesn't exist
	 */
	boolean create(long session, Node node) throws RecordNotFoundException;

	/**
	 * Deletes node
	 *
	 * @return
	 */
	boolean delete(Node node);

	/**
	 * Returns node that corresponds to requested path
	 *
	 * @param path
	 * @return
	 * @throws RecordNotFoundException
	 */
	Node get(String path) throws RecordNotFoundException;

	/**
	 * Updates node data on requested path.
	 * Stores new node version and node modification time.
	 */
	void update(String path, byte[] data, int newVersion, long modificationTime);

	/**
	 * Returns list of the children node names. Child node name is a relative name to the parent node.
	 *
	 * @param parent
	 * @return
	 * @throws RecordNotFoundException
	 */
	List<String> getChildren(Node parent) throws RecordNotFoundException;

	void updateCVersion(String path, int cversion);

	/**
	 * Returns list of epephemeral nodes of the requested session
	 *
	 * @param session
	 * @return
	 */
	List<String> getEphemeralPaths(long session);
}
