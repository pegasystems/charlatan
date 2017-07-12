package org.apache.zookeeper.dao.bean;

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

	public Node(String path) {
		this(path, null);
	}

	public Node(String path, byte[] data) {
		this(path, data, -1, 0, null);
	}

	public Node(String path, byte[] data, int version) {
		this(path, data, version, 0, null);
	}

	public Node(String path, byte[] data, CreateMode mode) {
		this(path, data, -1, 0, mode);
	}

	public Node(String path, byte[] data, int version, long timestamp, CreateMode mode) {

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
}
