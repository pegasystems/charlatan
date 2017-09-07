package com.pega.zooikeeper.server.io;

import com.pega.zooikeeper.io.ZookeeperWriter;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class SetDataResponse extends Response {
	private org.apache.zookeeper.data.Stat stat;

	public SetDataResponse(Stat stat) {
		this.stat = stat;
	}

	public Stat getStat() {
		return stat;
	}

	public void setStat(Stat stat) {
		this.stat = stat;
	}

	public void serialize(ZookeeperWriter writer) throws IOException {
		super.serialize(writer);
		stat.serialize(writer);
	}

}
