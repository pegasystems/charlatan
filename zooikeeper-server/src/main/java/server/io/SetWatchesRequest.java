package server.io;

import com.pega.zooikeeper.io.Deserializable;
import com.pega.zooikeeper.io.ZookeeperReader;

import java.util.List;

public class SetWatchesRequest implements Deserializable {
	private java.util.List<String> dataWatches;
	private java.util.List<String> existWatches;
	private java.util.List<String> childWatches;
	private long relativeZxid;

	public List<String> getDataWatches() {
		return dataWatches;
	}

	public void setDataWatches(List<String> dataWatches) {
		this.dataWatches = dataWatches;
	}

	public List<String> getExistWatches() {
		return existWatches;
	}

	public void setExistWatches(List<String> existWatches) {
		this.existWatches = existWatches;
	}

	public List<String> getChildWatches() {
		return childWatches;
	}

	public void setChildWatches(List<String> childWatches) {
		this.childWatches = childWatches;
	}

	public void deserialize(ZookeeperReader reader) throws java.io.IOException {

		relativeZxid = reader.readLong();

		dataWatches = reader.readVector();
		existWatches = reader.readVector();
		childWatches = reader.readVector();

	}

	public long getRelativeZxid() {
		return relativeZxid;
	}

	public void setRelativeZxid(long relativeZxid) {
		this.relativeZxid = relativeZxid;
	}
}
