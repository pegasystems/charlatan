package com.pega.charlatan.node.bean;

import org.apache.zookeeper.Watcher.Event.EventType;

import java.util.Objects;

/**
 * Created by natalia on 7/17/17.
 */
public class NodeUpdate {
	private EventType eventType;
	private String path;
	private long timestamp;
	private int emitterId;

	public NodeUpdate() {
	}

	public NodeUpdate(EventType eventType, String path, long timestamp, int emitterId) {

		this.eventType = eventType;
		this.path = path;
		this.timestamp = timestamp;
		this.emitterId = emitterId;
	}

	public EventType getEventType() {
		return eventType;
	}

	public void setEventType(EventType eventType) {
		this.eventType = eventType;
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
		return eventType + " [" + path + "]";
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof NodeUpdate) {
			NodeUpdate other = (NodeUpdate) obj;
			return Objects.equals(eventType, other.getEventType()) && Objects.equals(path, other.getPath()) && Objects.equals(timestamp, other.getTimestamp())
					&& Objects.equals(emitterId, other.getEmitterId());
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(eventType, emitterId, path, timestamp);
	}
}
