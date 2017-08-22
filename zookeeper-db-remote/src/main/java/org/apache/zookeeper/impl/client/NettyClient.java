package org.apache.zookeeper.impl.client;

import org.apache.zookeeper.impl.node.dao.RecordNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class NettyClient {

	protected static Logger logger = LoggerFactory.getLogger(NettyClient.class.getName());

	private ZookeeperDaoRemoteConnection connection;

	NettyClient() {
		this.connection = ZookeeperDaoRemoteConnection.getConnection();
	}

	public void sendMessage(HttpMethod httpMethod, String api) throws IOException, RecordNotFoundException {
		sendMessage(httpMethod, api, null);
	}

	public <T> T sendMessage(HttpMethod httpMethod, String api, Class<T> responseType) throws IOException, RecordNotFoundException {
		return sendMessage(httpMethod, api, null, responseType);
	}

	public <T> T sendMessage(HttpMethod httpMethod, String api, Object obj, Class<T> responseType) throws IOException, RecordNotFoundException {
		return connection.sendMessage(org.jboss.netty.handler.codec.http.HttpMethod.valueOf(httpMethod.name()), api, obj, responseType);
	}

}
