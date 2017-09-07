package com.pega.zooikeeper.server.io;

import com.pega.zooikeeper.io.Serializable;
import com.pega.zooikeeper.io.ZookeeperWriter;

import java.io.IOException;

public class ConnectResponse implements Serializable {
	private int protocolVersion = 0;
	private int timeOut = 6000;
	private long sessionId;
	private byte[] password;
	private boolean readOnly;

	public ConnectResponse(int protocolVersion, int timeOut, long sessionId, byte[] password, boolean readOnly) {
		this.protocolVersion = protocolVersion;
		this.timeOut = timeOut;
		this.sessionId = sessionId;
		this.password = password;
		this.readOnly = readOnly;
	}

	public int getProtocolVersion() {
		return protocolVersion;
	}

	public void setProtocolVersion(int protocolVersion) {
		this.protocolVersion = protocolVersion;
	}

	public int getTimeOut() {
		return timeOut;
	}

	public void setTimeOut(int timeOut) {
		this.timeOut = timeOut;
	}

	public long getSessionId() {
		return sessionId;
	}

	public void setSessionId(long sessionId) {
		this.sessionId = sessionId;
	}

	public byte[] getPassword() {
		return password;
	}

	public void setPassword(byte[] password) {
		this.password = password;
	}

	public boolean isReadOnly() {
		return readOnly;
	}

	public void setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
	}

	public void serialize(ZookeeperWriter writer) throws IOException {
		writer.writeInt(0); //place holder for size
		writer.writeInt(protocolVersion); //4 bytes
		writer.writeInt(timeOut); // 4 bytes
		writer.writeLong(sessionId); // 8 bytes
		writer.writeInt(0x10); //what is this?
		writer.write(password); // 16 bytes
		writer.writeBoolean(readOnly); // 1 byte
	}
}
