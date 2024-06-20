package com.pega.charlatan.node.bean;


import java.util.List;
import java.util.Objects;

/**
 * Created by natalia on 7/11/17.
 */
public class Node {

	private static char SEP = '/';

	private CreateMode mode;

	/**
	 * List of children paths relative to the Node
	 */
	private List<String> children;

	/**
	 * Full path of the node
	 */
	private String path;

	/**
	 * Node data
	 */
	private byte[] data;

	private NodeState state;

	public Node(){}

	public Node(String path) {
		this(path, null, CreateMode.PERSISTENT);
	}

	public Node(String path, byte[] data, CreateMode mode) {

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
		this.mode = mode;
		this.state = new NodeState();
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte data[]) {
		this.data = data;
	}

	public String getParentPath() {
		if (isRoot()) {
			return null;
		}

		int lastCh = path.lastIndexOf(SEP);

		if (lastCh == 0) {
			return String.valueOf(SEP);
		} else {
			return path.substring(0, lastCh);
		}
	}

	public boolean isRoot() {
		return path.length() == 1;
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
			return Objects.equals(this.path, n.path)
					&& Objects.equals(this.children, n.children)
					&& Objects.equals(this.data, n.data)
					&& Objects.equals(this.state, n.state)
					&& Objects.equals(this.mode, n.mode);
		}

		return false;
	}

	public List<String> getChildren() {
		return children;
	}

	public void setChildren(List<String> children) {
		this.children = children;
	}

	public NodeState getState() {
		return state;
	}
}
