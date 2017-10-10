package com.pega.charlatan.server.io;

import com.pega.charlatan.io.Deserializable;
import com.pega.charlatan.io.ZookeeperReader;

import java.io.IOException;

public class GetChildrenRequest implements Deserializable {
	private String path;
	private boolean watch;

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public boolean isWatch() {
		return watch;
	}

	public void setWatch(boolean watch) {
		this.watch = watch;
	}

	@Override
	public void deserialize(ZookeeperReader reader) throws IOException {
		path=reader.readString();
		watch=reader.readBoolean();
	}

	@Override
	public String toString(){
		return String.format("GetChildren: %s, watch: %b", path, watch);
	}
}
