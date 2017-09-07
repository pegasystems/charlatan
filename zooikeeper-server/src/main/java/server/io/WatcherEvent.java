package server.io;

import com.pega.zooikeeper.io.ZookeeperWriter;

import java.io.IOException;

public class WatcherEvent extends Response {
	private int type;
	private int state;
	private String path;

	public WatcherEvent(int type, int state, String path) {
		this.type = type;
		this.state = state;
		this.path = path;
	}

	public int getType() {
		return type;
	}

	public void setType(int type) {
		this.type = type;
	}

	public int getState() {
		return state;
	}

	public void setState(int state) {
		this.state = state;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	@Override
	public void serialize(ZookeeperWriter writer) throws IOException {
		super.serialize(writer);
		writer.writeInt(type);
		writer.writeInt(state);
		writer.writeString(path);
	}
}
