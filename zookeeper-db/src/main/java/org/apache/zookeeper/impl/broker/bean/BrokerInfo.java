package org.apache.zookeeper.impl.broker.bean;

/**
 * Created by natalia on 7/24/17.
 */
public class BrokerInfo {
	private final int brokerId;
	private final long session;
	private final long lastTimeSeen;

	public BrokerInfo(int brokerId, long session, long lastTimeSeen) {
		this.brokerId = brokerId;
		this.session = session;
		this.lastTimeSeen = lastTimeSeen;
	}

	public int getBrokerId() {
		return brokerId;
	}

	public long getSession() {
		return session;
	}

	public long getLastTimeSeen() {
		return lastTimeSeen;
	}
}
