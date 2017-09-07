package com.pega.zooikeeper.server.io;

import com.pega.zooikeeper.io.Serializable;
import com.pega.zooikeeper.io.ZookeeperWriter;

import java.io.IOException;

public class Response implements Serializable {
	private int transactionId;
	private long zid;
	private int errorCode;

	public int getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(int transactionId) {
		this.transactionId = transactionId;
	}

	public long getZid() {
		return zid;
	}

	public void setZid(long zid) {
		this.zid = zid;
	}

	public int getErrorCode() {
		return errorCode;
	}

	public void setErrorCode(int errorCode) {
		this.errorCode = errorCode;
	}

	public void serialize(ZookeeperWriter writer) throws IOException {
		writer.writeInt(0); //size place holder
		writer.writeInt(transactionId); //4 bytes
		writer.writeLong(zid); // 8 bytes
		writer.writeInt(errorCode); //what is this?
	}
}
