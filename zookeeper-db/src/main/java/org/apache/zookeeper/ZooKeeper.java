/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper;

import org.apache.zookeeper.dao.NodeDao;
import org.apache.zookeeper.dao.NodeDaoSqlite;
import org.apache.zookeeper.dao.RecordNotFoundException;
import org.apache.zookeeper.dao.bean.Node;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class ZooKeeper {

	// TODO: inject or load from config
	private NodeDao nodeDao = new NodeDaoSqlite( "/home/natalia/test.db" );

	private Logger logger = LoggerFactory.getLogger(ZooKeeper.class.getName());
	private ExecutorService executor;

	public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher) {
		executor = Executors.newCachedThreadPool();
	}

	public void close() {

	}

	/**
	 * Create a node with the given path.
	 * If a node with the same actual path already exists in the ZooKeeper, a
	 * KeeperException with error code KeeperException.NodeExists will be
	 * thrown.
	 *
	 * @param path
	 * @param data
	 * @param acl
	 * @param createMode
	 * @return
	 */
	public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException {
		Node node = new Node(path, data, createMode);
		try {
			if(nodeDao.create(node)) {
				return node.getPath();
			}

			throw new KeeperException.NodeExistsException();
		} catch (RecordNotFoundException e) {
			throw new KeeperException.NoNodeException(node.getParent().getPath());
		}
	}

	/**
	 * The version is ignored
	 *
	 * @param path
	 * @param version
	 * @throws KeeperException
	 */
	public void delete(String path, int version) throws KeeperException {
		Node node = new Node(path);
		node.setVersion(version);
		if (!nodeDao.delete(node)) {
			throw new KeeperException.NoNodeException(path);
		}
	}

	public List<String> getChildren(String path, boolean watch) throws KeeperException {
		try {
			return nodeDao.getChildren(new Node(path));
		} catch (RecordNotFoundException e) {
			throw new KeeperException.NoNodeException(path);
		}
	}

	public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException {
		try {
			Node node =  nodeDao.get(path);
			stat.setVersion(node.getVersion());
			stat.setCtime(node.getTimestamp());
			return node.getData();
		} catch (RecordNotFoundException e) {
			throw new KeeperException.NoNodeException(path);
		}
	}

	public void getData(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx) {
		executor.submit(() ->
		{
			try {
				byte[] data = getData(path, watch, null);
				cb.processResult(KeeperException.Code.OK.intValue(), path, ctx, data, new Stat());
			} catch (KeeperException e) {
				cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null, new Stat());
			}
		});

	}

	public Stat setData(String path, byte[] data, int version) throws KeeperException {
		try {
			nodeDao.update(new Node(path, data, version ));
		} catch (RecordNotFoundException e) {
			throw new KeeperException.NoNodeException(path);
		}
		return new Stat();
	}

	public Stat exists(String path, boolean watch) {
		logger.info( "Checking node " + path );
			if (nodeDao.exists(new Node(path))) {
			return new Stat();
		}
		return null;
	}

	public Stat exists(String path, Watcher watcher) {
		logger.info( "Checking node " + path );
		if (nodeDao.exists(new Node(path))) {
			return new Stat();
		}
		return null;
	}

	public void setACL(String path, List<ACL> acl, int version, AsyncCallback.StatCallback cb, Object ctx) {

	}


	/**
	 * The asynchronous version of getChildren. Return the list of the children of the node of the given path.
	 *
	 * @param path
	 * @param watch
	 * @param cb
	 * @param ctx
	 */
	public void getChildren(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx) {
		executor.submit(() ->
		{
			try {
				List<String> children = getChildren(path, watch);
				cb.processResult(KeeperException.Code.OK.intValue(), path, ctx, children);
			} catch (KeeperException e) {
				cb.processResult(KeeperException.Code.NONODE.intValue(), path, ctx, null);
			}
		});
	}

	public void create(final String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctx) {
		executor.submit(() ->
		{
			try {
				String node = create(path, data, acl, createMode);
				cb.processResult(KeeperException.Code.OK.intValue(), path, ctx, node);
			} catch (KeeperException e) {
				cb.processResult(KeeperException.Code.NODEEXISTS.intValue(), path, ctx, null);
			}
		});
	}

	/**
	 * The session id for this ZooKeeper client instance.
	 *
	 * @return
	 */
	public long getSessionId() {
		return 0;
	}

	public enum States {
		CONNECTING, ASSOCIATING, CONNECTED, CONNECTEDREADONLY,
		CLOSED, AUTH_FAILED, NOT_CONNECTED;

		public boolean isAlive() {
			return this != CLOSED && this != AUTH_FAILED;
		}

		/**
		 * Returns whether we are connected to a server (which
		 * could possibly be read-only, if this client is allowed
		 * to go to read-only mode)
		 */
		public boolean isConnected() {
			return this == CONNECTED || this == CONNECTEDREADONLY;
		}
	}

}
