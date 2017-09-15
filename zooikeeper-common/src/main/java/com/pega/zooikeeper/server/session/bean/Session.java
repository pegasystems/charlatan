package com.pega.zooikeeper.server.session.bean;

import java.util.Objects;
import java.util.UUID;

public class Session {

	private UUID uuid;
	private long sessionId;
	private int timeout;
	private long startTime;
	private long lastTimeSeen;

	public Session(UUID uuid, long startTime) {
		this.uuid = uuid;
		this.startTime = startTime;
		this.lastTimeSeen = startTime;
	}

	public long getSessionId() {
		return sessionId;
	}

	public void setSessionId(long sessionId) {
		this.sessionId = sessionId;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public long getLastTimeSeen() {
		return lastTimeSeen;
	}

	public void setLastTimeSeen(long lastTimeSeen) {
		this.lastTimeSeen = lastTimeSeen;
	}

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public UUID getUuid() {
		return uuid;
	}

	public void setUuid(UUID uuid) {
		this.uuid = uuid;
	}

	@Override
	public String toString() {
		return "Session: " + sessionId + ", UUID: " + uuid;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof Session) {
			Session other = (Session) obj;
			return Objects.equals(uuid, other.getUuid()) &&
					Objects.equals(sessionId, other.getSessionId()) &&
					Objects.equals(timeout, other.getTimeout()) &&
					Objects.equals(startTime, other.getStartTime()) &&
					Objects.equals(lastTimeSeen, other.getLastTimeSeen());
		}
		return false;
	}

	@Override
	public int hashCode(){
		return Objects.hash(uuid, sessionId, timeout, startTime, lastTimeSeen);
	}
}
