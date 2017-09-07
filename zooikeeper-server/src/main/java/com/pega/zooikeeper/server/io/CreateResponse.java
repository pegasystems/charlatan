package com.pega.zooikeeper.server.io;

import com.pega.zooikeeper.io.ZookeeperWriter;

import java.io.IOException;

public class CreateResponse extends Response {
	private String path;

	public CreateResponse(String path){
		this.path = path;
	}

	public String getPath() {
		return path;
	}


	@Override
	public void serialize(ZookeeperWriter writer) throws IOException {
		super.serialize(writer);
		writer.writeString(path);
	}
}
