package com.pega.charlatan.server.io;

import com.pega.charlatan.io.ZookeeperWriter;
import com.pega.charlatan.node.bean.NodeState;


import java.io.IOException;

public class SetDataResponse extends Response {
	private NodeState nodeState;

	public SetDataResponse(NodeState nodeState) {
		this.nodeState = nodeState;
	}

	public NodeState getNodeState() {
		return nodeState;
	}

	public void setNodeState(NodeState nodeState) {
		this.nodeState = nodeState;
	}

	public void serialize(ZookeeperWriter writer) throws IOException {
		super.serialize(writer);
		nodeState.serialize(writer);
	}

}
