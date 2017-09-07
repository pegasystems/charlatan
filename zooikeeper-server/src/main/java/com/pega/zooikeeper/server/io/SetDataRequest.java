package com.pega.zooikeeper.server.io;

import com.pega.zooikeeper.io.Deserializable;
import com.pega.zooikeeper.io.ZookeeperReader;

import java.io.IOException;

public class SetDataRequest implements Deserializable {

	private String path;
	private byte[] data;
	private int version;

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public int getVersion() {
		return version;
	}

	public void setVersion(int version) {
		this.version = version;
	}

	@Override
	public void deserialize(ZookeeperReader reader) throws IOException {
		path = reader.readString();
		data = reader.readBuffer();
		version = reader.readInt();
	}
}
