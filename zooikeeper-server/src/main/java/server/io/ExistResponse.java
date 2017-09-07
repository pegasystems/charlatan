package server.io;

import com.pega.zooikeeper.io.ZookeeperWriter;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class ExistResponse extends Response{
	private Stat stat;

	public ExistResponse(Stat stat) {
		this.stat = stat;
	}

	@Override
	public void serialize(ZookeeperWriter writer) throws IOException {
		super.serialize(writer);
		stat.serialize(writer);
	}
}
