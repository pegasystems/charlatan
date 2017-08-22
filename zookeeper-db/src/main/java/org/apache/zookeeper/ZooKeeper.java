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

import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.impl.broker.service.BrokerMonitorService;
import org.apache.zookeeper.impl.common.ZookeeperClassLoader;
import org.apache.zookeeper.impl.node.bean.Node;
import org.apache.zookeeper.impl.node.service.NodeService;
import org.apache.zookeeper.impl.node.service.NodeServiceImpl;
import org.apache.zookeeper.impl.node.service.ZKDatabase;
import org.apache.zookeeper.impl.watches.service.ClientWatchManagerImpl;

import java.io.IOException;
import java.util.List;


public class ZooKeeper implements NodeService {

	private NodeService nodeService;

	public ZooKeeper(String connectString, int sessionTimeout, Watcher watcher) throws IOException {

		if(connectString != null){
			String[] address = connectString.split(":");
			System.setProperty("ZOOKEEPER_HOST",address[0]);
			System.setProperty("ZOOKEEPER_PORT",address[1]);
		}

		this.nodeService = new NodeServiceImpl(
				new ZKDatabase(ZookeeperClassLoader.getNodeDao()),
				ZookeeperClassLoader.getRemoteNodeUpdates(),
				new ClientWatchManagerImpl(watcher, false),
				new BrokerMonitorService(ZookeeperClassLoader.getBrokerDaoImpl(), this, sessionTimeout));
	}

	public ZooKeeper(NodeService nodeService) {
		this.nodeService = nodeService;
	}

	@Override
	public void close() {
		nodeService.close();
	}

	@Override
	public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException {
		return nodeService.create(path, data, acl, createMode);
	}


	@Override
	public void create(final String path, byte[] data, List<ACL> acl, CreateMode createMode, AsyncCallback.StringCallback cb, Object ctx) {
		nodeService.create(path, data, acl, createMode, cb, ctx);
	}

	@Override
	public void delete(String path, int version) throws KeeperException {
		nodeService.delete(path, version);
	}

	@Override
	public List<String> getChildren(String path, boolean watch) throws KeeperException {
		return nodeService.getChildren(path, watch);
	}

	@Override
	public byte[] getData(String path, boolean watch, Stat stat) throws KeeperException {
		return nodeService.getData(path, watch, stat);
	}

	@Override
	public void getData(String path, boolean watch, AsyncCallback.DataCallback cb, Object ctx) {
		nodeService.getData(path, watch, cb, ctx);
	}

	private Node getNode(String path, boolean watch) throws KeeperException.NoNodeException {
		return getNode(path, watch);
	}


	@Override
	public Stat setData(String path, byte[] data, int version) throws KeeperException {
		return nodeService.setData(path, data, version);
	}

	@Override
	public Stat exists(String path, boolean watch) {
		return nodeService.exists(path, watch);
	}

	@Override
	public Stat exists(String path, Watcher watcher) {
		return nodeService.exists(path, watcher);
	}

	@Override
	public void setACL(String path, List<ACL> acl, int version, AsyncCallback.StatCallback cb, Object ctx) {
		nodeService.setACL(path, acl, version, cb, ctx);
	}


	@Override
	public void getChildren(String path, boolean watch, AsyncCallback.ChildrenCallback cb, Object ctx) {
		nodeService.getChildren(path, watch, cb, ctx);
	}

	@Override
	public long getSessionId() {
		return nodeService.getSessionId();
	}

	@Override
	public void removeSessionNodes(long session) {
		nodeService.removeSessionNodes(session);
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
