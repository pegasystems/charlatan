package org.apache.zookeeper.impl.node.bean;

import org.apache.zookeeper.CreateMode;

import java.util.Objects;

/**
 * Created by natalia on 7/11/17.
 */
public class Node {

	private static char SEP = '/';
	private String path;
	private byte[] data;
	private CreateMode mode;
	private long timestamp;
	private int version;
	private int cversion;
	private long ownerSession;

	public Node(String path) {
		this(path, null);
	}

	public Node(String path, byte[] data) {
		this(path, data, 0,0, 0, null);
	}

	public Node(String path, byte[] data, int version) {
		this(path, data, version, 0,0, null);
	}

	public Node(String path, byte[] data, CreateMode mode) {
		this(path, data, 0,0, 0, mode);
	}

	public Node(String path, byte[] data, int version, int cversion, long timestamp, CreateMode mode) {

		if (path == null) {
			throw new IllegalArgumentException("Path cannot be null");
		}
		if (path.length() == 0) {
			throw new IllegalArgumentException("Path length must be > 0");
		}

		if (path.charAt(0) != SEP) {
			throw new IllegalArgumentException("Path should start with '/' character");
		}

		if (path.length() > 1) {
			if (path.charAt(path.length() - 1) == SEP) {
				path = path.substring(0, path.lastIndexOf(SEP));
			}
		}

		this.path = path;
		this.data = data;
		this.version = version;
		this.cversion = cversion;
		this.timestamp = timestamp;
		this.mode = mode;
	}

	public String getPath() {
		return path;
	}

	public byte[] getData() {
		return data;
	}

	public Node getParent() {
		if (isRoot()) {
			return null;
		}

		int lastCh = path.lastIndexOf(SEP);
		if (lastCh == 0) {
			return new Node(String.valueOf(SEP));
		} else {
			return new Node(path.substring(0, lastCh + (lastCh == 0 ? 1 : 0)));
		}
	}

	public boolean isRoot() {
		return path.length() == 1;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	public boolean hasVersion() {
		return version >= 0;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public CreateMode getMode() {
		return mode;
	}

	public void setMode(CreateMode mode) {
		this.mode = mode;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Node) {
			Node n = (Node) obj;
			return Objects.equals(this.version, n.version) && Objects.equals(this.path, n.path)
					&& Objects.equals(this.data, n.data)
					&& Objects.equals(this.timestamp, n.timestamp);
		}

		return false;
	}

	public long getOwnerSession() {
		return ownerSession;
	}

	public void setOwnerSession(long ownerSession) {
		this.ownerSession = ownerSession;
	}

	public int getCversion() {
		return cversion;
	}

	public void setCversion(int cversion) {
		this.cversion = cversion;
	}

	public void setPath(String path) {
		this.path = path;
	}
}
