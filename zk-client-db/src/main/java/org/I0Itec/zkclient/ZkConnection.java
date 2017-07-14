package org.I0Itec.zkclient;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.util.List;
import java.util.Map;

/**
 * Created by natalia on 7/10/17.
 */
public class ZkConnection implements IZkConnection {

	private ZooKeeper zk;

	public ZkConnection(String zkServers, int sessionTimeout) {

	}

	public ZooKeeper getZookeeper() {
		return zk;
	}

	@Override
	public void connect(Watcher watcher) {
		zk = new ZooKeeper(null, 0, null);
	}

	@Override
	public void close() {
		zk.close();
	}

	@Override
	public String create(String path, byte[] data, CreateMode mode) throws KeeperException {
		return zk.create(path, data, null, mode);
	}

	@Override
	public String create(String path, byte[] data, List<ACL> acl, CreateMode mode) throws KeeperException {
		return zk.create(path, data, acl, mode);
	}

	@Override
	public void delete(String path) throws InterruptedException, KeeperException {
		zk.delete(path, -1);
	}

	@Override
	public void delete(String path, int version) throws InterruptedException, KeeperException {
		zk.delete(path, version);
	}

	@Override
	public boolean exists(String path, boolean watch) throws KeeperException, InterruptedException {
		return zk.exists(path, watch) != null;
	}

	@Override
	public List<String> getChildren(String path, boolean watch) throws KeeperException {
		return zk.getChildren(path, watch);
	}

	@Override
	public byte[] readData(String path, Stat stat, boolean watch) throws KeeperException, InterruptedException {
		return zk.getData(path, watch, stat);
	}

	@Override
	public void writeData(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException {
		zk.setData(path, data, expectedVersion);
	}

	@Override
	public Stat writeDataReturnStat(String path, byte[] data, int expectedVersion) throws KeeperException, InterruptedException {
		return zk.setData(path, data, expectedVersion);
	}

	@Override
	public ZooKeeper.States getZookeeperState() {
		return ZooKeeper.States.CONNECTED;
	}

	@Override
	public long getCreateTime(String path) throws KeeperException, InterruptedException {
		return 0;
	}

	@Override
	public String getServers() {
		return null;
	}

	@Override
	public void addAuthInfo(String scheme, byte[] auth) {

	}

	@Override
	public void setAcl(String path, List<ACL> acl, int version) throws KeeperException, InterruptedException {

	}


	@Override
	public Map.Entry<List<ACL>, Stat> getAcl(String path) throws KeeperException, InterruptedException {
		return null;
	}
}
