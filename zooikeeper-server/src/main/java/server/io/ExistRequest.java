package server.io;

import com.pega.zooikeeper.io.Deserializable;
import com.pega.zooikeeper.io.ZookeeperReader;

import java.io.IOException;

public class ExistRequest implements Deserializable {
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
}
