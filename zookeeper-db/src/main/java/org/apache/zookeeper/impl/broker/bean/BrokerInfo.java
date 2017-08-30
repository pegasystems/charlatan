package org.apache.zookeeper.impl.broker.bean;

import java.util.Objects;

/**
 * Created by natalia on 7/24/17.
 */
public class BrokerInfo {
	private int brokerId;
	private long session;
	private long lastTimeSeen;

	public BrokerInfo() {
	}

	public BrokerInfo(int brokerId, long session, long lastTimeSeen) {
		this.brokerId = brokerId;
		this.session = session;
		this.lastTimeSeen = lastTimeSeen;
	}

	public int getBrokerId() {
		return brokerId;
	}

	public void setBrokerId(int brokerId) {
		this.brokerId = brokerId;
	}

	public long getSession() {
		return session;
	}

	public void setSession(long session) {
		this.session = session;
	}

	public long getLastTimeSeen() {
		return lastTimeSeen;
	}

	public void setLastTimeSeen(long lastTimeSeen) {
		this.lastTimeSeen = lastTimeSeen;
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof BrokerInfo) {
			BrokerInfo other = (BrokerInfo) obj;
			return Objects.equals(brokerId, other.getBrokerId()) && Objects.equals(session, other.getSession()) && Objects.equals(lastTimeSeen, other.getLastTimeSeen());
		}
		return false;
	}

	@Override
	public int hashCode(){
		return Objects.hash(brokerId, session, lastTimeSeen);
	}
}
