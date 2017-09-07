package server.io;

import com.pega.zooikeeper.io.ZookeeperWriter;

import java.io.IOException;
import java.util.List;

public class GetChildrenResponse extends Response {

	private List<String> children;

	public GetChildrenResponse(List<String> children) {
		this.children = children;
	}

	public void serialize(ZookeeperWriter writer) throws IOException {
		super.serialize(writer);
		writer.writeVector(children);
	}
}
