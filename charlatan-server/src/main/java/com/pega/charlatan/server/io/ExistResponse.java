package com.pega.charlatan.server.io;

import com.pega.charlatan.io.ZookeeperWriter;
import com.pega.charlatan.node.bean.NodeState;

import java.io.IOException;

public class ExistResponse extends Response{
	private NodeState nodeState;

	public ExistResponse(NodeState nodeState) {
		this.nodeState = nodeState;
	}

	@Override
	public void serialize(ZookeeperWriter writer) throws IOException {
		super.serialize(writer);
		nodeState.serialize(writer);
	}
}
