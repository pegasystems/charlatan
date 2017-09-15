/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.pega.zooikeeper.server;

import com.pega.zooikeeper.io.Serializable;
import com.pega.zooikeeper.io.ZookeeperReader;
import com.pega.zooikeeper.io.ZookeeperWriter;
import com.pega.zooikeeper.node.service.NodeService;
import com.pega.zooikeeper.server.io.*;
import com.pega.zooikeeper.server.session.bean.Session;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

import static org.jboss.netty.buffer.ChannelBuffers.wrappedBuffer;

/**
 * Channel wrapper that keeps session specific information like: sessionId, session read timeout.
 * ZooikeeperNettyConnection handles read timeout that is set during the connect handshake.
 */
public class ZooikeeperNettyConnection implements Watcher {

	public static final int DEFAULT_MAX_SESSION_TIMEOUT = 60000;

	private static final Timer TIMER = new Timer();
	private static final Logger logger = LoggerFactory.getLogger(ZooikeeperNettyConnection.class);
	volatile boolean closingChannel;
	private Channel channel;

	// Session information
	private Session session;
	private long zid;
	// True if connect request was processed
	private boolean initialized;
	// It is used to monitor read timeout
	private TimerTask timerTask;
	private NodeService nodeService;
//
//	ZooikeeperNettyConnection(Channel channel, NodeService nodeService) {
//		this(channel,nodeService, new Session(UUID.randomUUID(), System.currentTimeMillis()));
//	}

	ZooikeeperNettyConnection(Channel channel, NodeService nodeService, Session session) {
		this.channel = channel;
		this.closingChannel = false;
		this.nodeService = nodeService;
		this.session = session;
	}

	public Session getSession() {
		return session;
	}

	public void receiveMessage(ChannelBuffer message) {
		try {
			while (message.readable()) {

				session.setLastTimeSeen(System.currentTimeMillis());

				// read message length
				int length = message.readInt();

				ByteBuffer bb = ByteBuffer.allocate(length);
				message.readBytes(bb);
				bb.flip();

				if (initialized) {
					processPacket(bb);
				} else {
					processConnectRequest(bb);
					initialized = true;
				}
			}
		} catch (Exception e) {
			logger.warn("Closing connection to " + channel.getRemoteAddress(), e);
			close();
		}
	}

	private void processConnectRequest(ByteBuffer bb) {

		try {
			ConnectRequest connectRequest = new ConnectRequest();
			connectRequest.deserialize(new ZookeeperReader(new DataInputStream(new ByteArrayInputStream(bb.array()))));

			long sessionId = connectRequest.getSessionId();

			// accept only new session
			if (sessionId == 0) {
				Random random = new Random();

				sessionId = random.nextLong();
				byte[] password = new byte[16];
				random.nextBytes(password);

				session.setSessionId(sessionId);
				setTimeout(connectRequest.getTimeOut());

				this.zid = connectRequest.getZxid();


				ConnectResponse connectResponse = new ConnectResponse(connectRequest.getProtocolVersion(),
						session.getTimeout(),
						session.getSessionId(),
						password,
						connectRequest.isReadOnly());

				send(connectResponse);
				// notify client that the session isn't valid
			} else {
				ConnectResponse connectResponse = new ConnectResponse(0, 0, 0, // send 0 if session is no
						new byte[16], false);

				send(connectResponse);
				close();
			}
		} catch (Exception e) {
			logger.error("Unable to initialized the connection", e);
			close();
		}
	}

	private void processPacket(ByteBuffer bb) {
		KeeperException.Code errCode = KeeperException.Code.OK;
		Response response = null;
		int transactionId = -1;
		RequestType requestType = null;

		try {
			ZookeeperReader reader = new ZookeeperReader(new DataInputStream(new ByteArrayInputStream(bb.array())));

			transactionId = reader.readInt();
			int operationId = reader.readInt();
			requestType = RequestType.getRequestType(operationId);

			if (requestType == null) {
				logger.error(String.format("Dropping unknown request type with operation id %d", operationId));
				return;
			}

//			logger.debug(String.format("Requested: %s, id: %d", requestType.name(), transactionId));

			switch (requestType) {
				case SetWatches:
					response = processSetWatches(reader);
					break;
				case Ping:
					response = processPingRequest();
					break;
				case Exists:
					response = processExistsRequest(reader);
					break;
				case Create:
					response = processCreateRequest(reader);
					break;
				case GetData:
					response = processGetDataRequest(reader);
					break;
				case SetData:
					response = processSetDataRequest(reader);
					break;
				case GetChildren:
					response = processGetChildrenRequest(reader);
					break;
				case Delete:
					response = processDeleteRequest(reader);
					break;
				case CloseSession:
					response = processCloseSession();
				default:
					throw new KeeperException.SystemErrorException();
			}
		} catch (IOException e) {
			logger.error("Unable to process request", e);
			errCode = KeeperException.Code.MARSHALLINGERROR;
		} catch (KeeperException e) {
			errCode = e.code();
		}

		if (response == null) {
			response = new Response();
		}

		response.setTransactionId(transactionId);
		response.setZid(zid);
		response.setErrorCode(errCode.intValue());

		try {
			send(response);
		} catch (IOException e) {
			logger.error("Unable to send the response", e);
		}

		if (requestType == RequestType.CloseSession) {
			close();
		}
	}

	public void close() {
		logger.debug("Close called for session " + session);

		closingChannel = true;

		try {
			if (timerTask != null) {
				timerTask.cancel();
			}
		}
		catch( Exception e ){
			logger.warn(e.getMessage());
		}

		try {
			if (channel.isOpen()) {
				channel.write(ChannelBuffers.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
			}
		}
		catch( Exception e){
			logger.warn(e.getMessage());
		}

		nodeService.close(session.getSessionId());
	}

	private Response processPingRequest() {
		return new Response();
	}


	private Response processSetWatches(ZookeeperReader reader) throws IOException {
		SetWatchesRequest setWatches = new SetWatchesRequest();
		setWatches.deserialize(reader);

		logger.debug(setWatches.toString());
		nodeService.registerWatch(this, setWatches.getDataWatches(), setWatches.getChildWatches(), setWatches.getExistWatches());
		return new Response();
	}

	private Response processExistsRequest(ZookeeperReader reader) throws IOException, KeeperException.NoNodeException {
		ExistRequest existRequest = new ExistRequest();
		existRequest.deserialize(reader);

		Stat stat = nodeService.exists(existRequest.getPath(), existRequest.isWatch() ? this : null);

		logger.debug(existRequest.toString());

		if (stat == null) {
			throw new KeeperException.NoNodeException(existRequest.getPath());
		}

		return new ExistResponse(stat);
	}

	private Response processCreateRequest(ZookeeperReader reader) throws IOException, KeeperException {
		CreateRequest createRequest = new CreateRequest();
		createRequest.deserialize(reader);

		logger.debug(createRequest.toString());
		String nodePath = nodeService.create(session.getSessionId(), createRequest.getPath(), createRequest.getData(), createRequest.getAcl(), CreateMode.fromFlag(createRequest.getFlags()));

		return new CreateResponse(nodePath);
	}

	private Response processGetDataRequest(ZookeeperReader reader) throws IOException, KeeperException {
		GetDataRequest getDataRequest = new GetDataRequest();
		getDataRequest.deserialize(reader);

		Stat stat = new Stat();
		byte[] data = nodeService.getData(getDataRequest.getPath(), getDataRequest.isWatch() ? this : null, stat);

		logger.debug(getDataRequest.toString());

		GetDataResponse getDataResponse = new GetDataResponse();
		getDataResponse.setStat(stat);
		getDataResponse.setData(data);
		return getDataResponse;
	}


	private Response processSetDataRequest(ZookeeperReader reader) throws IOException, KeeperException {
		SetDataRequest setDataRequest = new SetDataRequest();
		setDataRequest.deserialize(reader);
		logger.debug(setDataRequest.toString());
		Stat stat = nodeService.setData(setDataRequest.getPath(), setDataRequest.getData(), setDataRequest.getVersion());
		return new SetDataResponse(stat);
	}

	private Response processGetChildrenRequest(ZookeeperReader reader) throws IOException, KeeperException {
		GetChildrenRequest getChildrenRequest = new GetChildrenRequest();
		getChildrenRequest.deserialize(reader);
		logger.debug(getChildrenRequest.toString());
		boolean watch = getChildrenRequest.isWatch();
		List<String> children = nodeService.getChildren(getChildrenRequest.getPath(), watch ? this : null);
		return new GetChildrenResponse(children);
	}

	private Response processDeleteRequest(ZookeeperReader reader) throws IOException, KeeperException {
		DeleteRequest deleteRequest = new DeleteRequest();
		deleteRequest.deserialize(reader);
		nodeService.delete(deleteRequest.getPath(), deleteRequest.getVersion());
		return new Response();
	}


	private Response processCloseSession() {
		nodeService.close(session.getSessionId());
		return new Response();
	}

	@Override
	public void process(WatchedEvent event) {
		// Convert WatchedEvent to a type that can be sent over the wire
		WatcherEvent e = new WatcherEvent(event.getType().getIntValue(), event.getState().getIntValue(), event.getPath());
		e.setTransactionId(-1);
		e.setZid(-1l);
		e.setErrorCode(0);

		try {
			if (closingChannel || !channel.isOpen()) {
				return;
			}
			ByteArrayOutputStream baos = new ByteArrayOutputStream();
			e.serialize(new ZookeeperWriter(new DataOutputStream(baos)));

			ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
			buffer.putInt(buffer.remaining() - 4).rewind();

			channel.write(wrappedBuffer(buffer));
		} catch (IOException e1) {
			close();
		}
	}

	private void send(Serializable response) throws IOException {

		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		response.serialize(new ZookeeperWriter(new DataOutputStream(baos)));

		ByteBuffer buffer = ByteBuffer.wrap(baos.toByteArray());
		buffer.putInt(buffer.remaining() - 4).rewind();

		channel.write(wrappedBuffer(buffer));
	}

	private void setTimeout(int timeout) {
		session.setTimeout(Math.min(timeout,DEFAULT_MAX_SESSION_TIMEOUT) );

		//start checking the timeout
		if (timerTask == null && session.getTimeout() > 0 && !closingChannel) {
			timerTask = new TimerTask() {
				@Override
				public void run() {
					if ((session.getLastTimeSeen() + session.getTimeout()) < System.currentTimeMillis()) {
						logger.info("Read timeout on " + channel.getRemoteAddress());
						close();
					}
					// Should we update session info in the db? If zooikeeper server crashes ephemeral nodes will not be deleted.
				}
			};
			TIMER.schedule(timerTask, timeout, 1000);
		}
	}
}

