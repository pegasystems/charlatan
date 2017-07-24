package org.apache.zookeeper.impl.node.service;

import org.apache.zookeeper.impl.node.bean.Node;
import org.apache.zookeeper.impl.node.bean.NodeTreeCache;
import org.apache.zookeeper.impl.node.dao.NodeDao;
import org.apache.zookeeper.impl.node.dao.RecordNotFoundException;

import java.util.List;

/**
 *
 */
public class ZKDatabase implements NodeDao {

	final private NodeDao nodeDao;
	final private NodeTreeCache nodeTree;

	public ZKDatabase(NodeDao nodeDao) {
		this.nodeDao = nodeDao;
		this.nodeTree = new NodeTreeCache();
	}

	@Override
	public boolean create(long session, Node node) throws RecordNotFoundException {
		if (nodeDao.create(session, node)) {
			nodeTree.setPathNode(node.getPath(), node);
			if(node.getMode().isEphemeral()){
				nodeTree.addSessionEphemeralPath(node.getPath());
			}
			return true;
		}

		return false;
	}

	@Override
	public boolean delete(Node node) {
		if( nodeDao.delete(node) ){
			nodeTree.removeNode(node.getPath());

			return true;
		}
		return false;
	}

	@Override
	public Node get(String path) throws RecordNotFoundException {
//		if (nodeTree.containsPath(path)) {
//			Node node = nodeTree.get(path);
//
//			if (node == null) {
//				throw new RecordNotFoundException("Node doesn't exist " + path);
//			}
//			return node;
//		} else {
			Node node = nodeDao.get(path);
			nodeTree.setPathNode(path, node);
			return node;
//		}
	}

	@Override
	public void update(String path, byte[] data, int newVersion, long modificationTime){
		nodeDao.update(path,data,newVersion, modificationTime);
		nodeTree.invalidate(path);
	}

	@Override
	public List<String> getChildren(Node parent) throws RecordNotFoundException {
		return nodeDao.getChildren(parent);
	}

	@Override
	public void updateCVersion(String path, int cversion) {
		nodeDao.updateCVersion(path,cversion);
		nodeTree.invalidate(path);
	}

	@Override
	public List<String> getEphemeralPaths(long session) {

		return nodeDao.getEphemeralPaths(session);
//		return nodeTree.getSessionEphemeralPaths();
	}
}
