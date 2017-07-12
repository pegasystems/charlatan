package org.I0Itec.zkclient;

import org.I0Itec.zkclient.exception.ZkException;
import org.I0Itec.zkclient.exception.ZkInterruptedException;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.exception.ZkNodeExistsException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UnsupportedEncodingException;
import java.util.List;

/**
 * Created by natalia on 7/10/17.
 */
public class ZkClient implements Watcher {

	private Logger logger = LoggerFactory.getLogger(ZkClient.class.getName());
	private IZkConnection connection;
	private long operationRetryTimeoutInMillis;

	public ZkClient(IZkConnection zkConnection, int connectionTimeout, ZkSerializer zkSerializer) {
		this(zkConnection, connectionTimeout, zkSerializer, -1);
	}

	public ZkClient(final IZkConnection zkConnection, final int connectionTimeout, final ZkSerializer zkSerializer, final long operationRetryTimeout) {
		this.connection = zkConnection;
		this.operationRetryTimeoutInMillis = operationRetryTimeout;

		connection.connect(null);
	}

	public ZkClient(String zkServers, int sessionTimeout, int connectionTimeout, ZkSerializer zkSerializer) {
		this(new ZkConnection(zkServers, sessionTimeout), connectionTimeout, zkSerializer);
	}

	@Override
	public void process(WatchedEvent event) {

	}

	public void close() {

	}

	public void deleteRecursive(String dir) {

	}

	public boolean exists(String path) {
		try {
			logger.info( "Connection" + connection );
			return connection.exists(path, false);
		} catch (Exception e) {
			logger.error( "Path doesn't exist " + path, e );
		}

		return false;
	}

	public void writeData(String path, Object object) {
		writeDataReturnStat(path, object, -1);
	}

	private byte[] serialize(Object object) {
		try {
			return object.toString().getBytes("UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new ArgumentRuntimeException("Unsupported encoding");
		}
	}

	private <T extends Object> T deserialize(byte[] data) {
		if (data == null) {
			return null;
		}
		try {
			return (T) new String(data, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new ArgumentRuntimeException("Unsupported encoding");
		}
	}

	public Stat writeDataReturnStat(final String path, Object data, final int expectedVersion) {
		try {
			return connection.writeDataReturnStat(path, serialize(data), expectedVersion);
		} catch (Exception e) {
			throw new ArgumentRuntimeException(e);
		}
	}

	public boolean delete(final String path) {
		return delete(path, -1);
	}

	public boolean delete(final String path, final int version) {
		try {
			connection.delete(path, version);
			return true;
		} catch (KeeperException e) {
			return false;
		} catch (InterruptedException e) {
			throw new ArgumentRuntimeException(e);
		}
	}

	public <T extends Object> T readData(String path) {
		return (T) readData(path, null);
	}

	/**
	 * @param path
	 * @param stat - stat of the node after reading
	 * @param <T>
	 * @return
	 */
	public <T extends Object> T readData(String path, Stat stat) {
		try {
			byte[] data = connection.readData(path, stat, false);
			return deserialize(data);
		} catch (KeeperException e) {
			throw ZkException.create(e);
		} catch (InterruptedException e) {
			throw new ArgumentRuntimeException(e);
		}
	}

	/**
	 * Create a persistent node.
	 */
	public void createPersistent(String path, Object data, List<ACL> acl) {
		try {
			connection.create(path,serialize(data), acl, null);
		} catch (KeeperException e) {
			throw ZkException.create(e);
		}
	}

	public void createPersistent(String path, boolean createParents, List<ACL> acl) throws ZkInterruptedException, IllegalArgumentException, ZkException {
		try {
			connection.create(path, null, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			if (!createParents) {
				throw ZkException.create(e);
			}

			String parentDir = path.substring(0, path.lastIndexOf('/'));
			createPersistent(parentDir, createParents, acl);
			createPersistent(path, createParents, acl);
		}
	}

	/**
	 * Create a persistent, sequential node and set its ACL.
	 */
	public String createPersistentSequential(String path, Object data, List<ACL> acl) throws  IllegalArgumentException, ZkException {
		try {
			connection.create(path, null, CreateMode.PERSISTENT_SEQUENTIAL);
		} catch (KeeperException e) {
			throw ZkException.create(e);
		}

		return path;
	}

	/**
	 * Create an ephemeral node.
	 */
	public void createEphemeral(final String path, final Object data, final List<ACL> acl) throws  IllegalArgumentException, ZkException {
		try {
			connection.create(path, null, CreateMode.EPHEMERAL);
		} catch (KeeperException e) {
			throw ZkException.create(e);
		}
	}

	public List<String> subscribeChildChanges(String path, IZkChildListener listener) {
		return getChildren(path);
	}

	public void unsubscribeChildChanges(String path, IZkChildListener childListener) {
	}

	public void subscribeStateChanges(final IZkStateListener listener) {

	}

	public void subscribeDataChanges(String path, IZkDataListener listener) {

	}

	public void unsubscribeDataChanges(String path, IZkDataListener dataListener) {

	}

	public List<String> getChildren(String path) {
		try {
			return connection.getChildren(path, false);
		} catch (KeeperException e) {
			throw ZkException.create(e);
		}
	}

	public void unsubscribeAll() {
		throw new FakeZookeeperException(FakeZookeeperException.Code.UNIMPLEMENTED);
	}
}
