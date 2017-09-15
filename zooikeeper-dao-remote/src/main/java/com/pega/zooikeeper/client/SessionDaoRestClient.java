package com.pega.zooikeeper.client;

import com.pega.zooikeeper.node.dao.DataAccessException;
import com.pega.zooikeeper.node.dao.RecordNotFoundException;
import com.pega.zooikeeper.server.session.bean.Session;
import com.pega.zooikeeper.server.session.dao.SessionDao;
import com.pega.zooikeeper.utils.Service;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@Service
public class SessionDaoRestClient extends NettyClient implements SessionDao {
	@Override
	public void insertSession(String serverId, Session session) {
		try {
			sendMessage(HttpMethod.PUT, "/sessions", session, null);
		} catch (IOException | RecordNotFoundException e) {
			logger.error("Failed to update broker info", e);
			throw new DataAccessException("Failed to update broker info", e);
		}
	}

	@Override
	public List<Session> getStaleSessions(long maxLastSeen) {
		try {
			return Arrays.asList(sendMessage(HttpMethod.GET, "/sessions?maxLastSeen=" + maxLastSeen, Session[].class));
		} catch (IOException | RecordNotFoundException e) {
			logger.error("Failed to retrieve broker info", e);
			throw new DataAccessException("Failed to get expired brokers", e);
		}
	}

	@Override
	public Session getSession(UUID uuid) throws RecordNotFoundException {
		return null;
	}

	@Override
	public void deleteSession(UUID uuid) {
		try {
			sendMessage(HttpMethod.DELETE, "/sessions/" + uuid.toString(), Boolean.class);
		} catch (IOException | RecordNotFoundException e) {
			logger.error("Failed to delete broker info", e);
			throw new DataAccessException("Failed to delete broker info", e);
		}
	}

	@Override
	public void updateSession(Session session) {
		try {
			sendMessage(HttpMethod.POST, "/sessions/" + session.getUuid().toString(), session, null);
		} catch (IOException | RecordNotFoundException e) {
			logger.error("Failed to update broker info", e);
			throw new DataAccessException("Failed to update broker info", e);
		}
	}
}
