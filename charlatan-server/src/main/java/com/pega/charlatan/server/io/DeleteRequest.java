package com.pega.charlatan.server.io;

import com.pega.charlatan.io.Deserializable;
import com.pega.charlatan.io.ZookeeperReader;

import java.io.IOException;

public class DeleteRequest implements Deserializable {

	private String path;
	private int version;

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
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
		version = reader.readInt();
	}
}
