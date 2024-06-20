package com.pega.charlatan.server.io;

import com.pega.charlatan.io.ZookeeperWriter;
import com.pega.charlatan.node.bean.NodeState;

import java.io.IOException;

public class GetDataResponse extends Response {
	private byte[] data;
	private NodeState nodeState;

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public NodeState getNodeState() {
		return nodeState;
	}

	public void setNodeState(NodeState nodeState) {
		this.nodeState = nodeState;
	}

	public void serialize(ZookeeperWriter writer) throws IOException {
		super.serialize(writer);
		writer.writeBuffer(data);
		nodeState.serialize(writer);
	}
}
