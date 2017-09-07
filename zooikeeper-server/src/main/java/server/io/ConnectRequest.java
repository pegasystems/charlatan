package server.io;

import com.pega.zooikeeper.io.Deserializable;
import com.pega.zooikeeper.io.ZookeeperReader;

import java.io.IOException;

public class ConnectRequest implements Deserializable {

	private int protocolVersion;
	private long zxid; //actually it is two ints
	private int timeOut;
	private long sessionId;
	private byte[] password;
	private boolean readOnly;

	public int getProtocolVersion() {
		return protocolVersion;
	}

	public void setProtocolVersion(int protocolVersion) {
		this.protocolVersion = protocolVersion;
	}

	public long getZxid() {
		return zxid;
	}

	public void setZxid(long zxid) {
		this.zxid = zxid;
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

	public void deserialize(ZookeeperReader reader) throws IOException {
		//int size = buff.readInt();
		protocolVersion = reader.readInt();
		zxid = reader.readLong();

		timeOut = reader.readInt();
		sessionId = reader.readLong();

		password = new byte[16];
		reader.readFully(password);

		readOnly = reader.readBoolean();
	}

	public boolean isReadOnly() {
		return readOnly;
	}

	public void setReadOnly(boolean readOnly) {
		this.readOnly = readOnly;
	}
}
