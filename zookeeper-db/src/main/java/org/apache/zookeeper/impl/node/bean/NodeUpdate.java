package org.apache.zookeeper.impl.node.bean;

import org.apache.zookeeper.Watcher.Event.EventType;

import java.util.Objects;

/**
 * Created by natalia on 7/17/17.
 */
public class NodeUpdate {
	private EventType eventType;
	private String path;
	private long timestamp;
	private int brokerId;

	public NodeUpdate() {
	}

	public NodeUpdate(EventType eventType, String path, long timestamp, int brokerId) {

		this.eventType = eventType;
		this.path = path;
		this.timestamp = timestamp;
		this.brokerId = brokerId;
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

	public int getBrokerId() {
		return brokerId;
	}

	public void setBrokerId(int brokerId) {
		this.brokerId = brokerId;
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
					&& Objects.equals(brokerId, other.getBrokerId());
		}
		return false;
	}

	@Override
	public int hashCode() {
		return Objects.hash(eventType, brokerId, path, timestamp);
	}
}
