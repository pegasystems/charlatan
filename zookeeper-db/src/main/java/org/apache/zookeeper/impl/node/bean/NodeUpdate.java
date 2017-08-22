package org.apache.zookeeper.impl.node.bean;

import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * Created by natalia on 7/17/17.
 */
public class NodeUpdate {
	private int id;
	private EventType eventType;
	private String path;
	private long timestamp;
	private int brokerId;

	public NodeUpdate() {
	}

	public NodeUpdate(EventType eventType, String path, long timestamp, int brokerId) {
		this(-1, eventType, path, timestamp, brokerId);
	}

	public NodeUpdate(int id, EventType eventType, String path, long timestamp, int brokerId) {

		this.id = id;
		this.eventType = eventType;
		this.path = path;
		this.timestamp = timestamp;
		this.brokerId = brokerId;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
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
}
