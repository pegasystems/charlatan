package server.io;

import com.pega.zooikeeper.io.ZookeeperWriter;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class GetDataResponse extends Response {
	private byte[] data;
	private org.apache.zookeeper.data.Stat stat;

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public Stat getStat() {
		return stat;
	}

	public void setStat(Stat stat) {
		this.stat = stat;
	}

	public void serialize(ZookeeperWriter writer) throws IOException {
		super.serialize(writer);
		writer.writeBuffer(data);
		stat.serialize(writer);
	}
}
