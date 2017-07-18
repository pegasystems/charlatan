package org.apache.zookeeper.bean;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;

/**
 * Created by natalia on 7/17/17.
 */
public class NodeUpdate {
	private int id;
	private EventType eventType;
	private String path;
	private long timestamp;

	public NodeUpdate(EventType eventType, String path, long timestamp) {
		this(-1, eventType, path, timestamp);
	}

	public NodeUpdate(int id, EventType eventType, String path, long timestamp) {

		this.id = id;
		this.eventType = eventType;
		this.path = path;
		this.timestamp = timestamp;
	}

	public int getId() {
		return id;
	}

	public EventType getEventType() {
		return eventType;
	}

	public String getPath() {
		return path;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString(){
		return eventType + " [" + path + "]";
	}
}
