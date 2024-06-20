package com.pega.charlatan.node.bean;

import com.pega.charlatan.watches.bean.Watcher.Event.Type;

import java.util.Objects;

/**
 * Created by natalia on 7/17/17.
 */
public class NodeUpdate {
	private Type type;
	private String path;
	private long timestamp;
	private int emitterId;

	public NodeUpdate() {
	}

	public NodeUpdate(Type type, String path, long timestamp, int emitterId) {

		this.type = type;
		this.path = path;
		this.timestamp = timestamp;
		this.emitterId = emitterId;
	}

	public Type getType() {
		return type;
	}

	public void setType(Type type) {
		this.type = type;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	public int getEmitterId() {
		return emitterId;
	}

	public void setEmitterId(int emitterId) {
		this.emitterId = emitterId;
	}

	@Override
	public String toString() {
		return type + " [" + path + "]";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof NodeUpdate) {
			NodeUpdate other = (NodeUpdate) obj;
			return Objects.equals(type, other.getType()) && Objects.equals(path, other.getPath()) && Objects.equals(timestamp, other.getTimestamp())
					&& Objects.equals(emitterId, other.getEmitterId());
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(type, emitterId, path, timestamp);
	}
}
