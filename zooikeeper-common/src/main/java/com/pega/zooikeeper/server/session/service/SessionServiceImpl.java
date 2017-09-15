package com.pega.zooikeeper.server.session.service;

import com.pega.zooikeeper.server.session.bean.Session;
import com.pega.zooikeeper.server.session.dao.SessionDao;

import java.util.List;
import java.util.UUID;

public class SessionServiceImpl implements SessionService {

	private final SessionDao sessionDao;

	public SessionServiceImpl(SessionDao sessionDao) {
		this.sessionDao = sessionDao;
	}

	@Override
	public void registerSession(String serverId, Session session) {
		sessionDao.insertSession(serverId, session);
	}

	@Override
	public void updateSession(Session session) {
		sessionDao.updateSession(session);
	}

	@Override
	public void deleteSession(UUID uuid) {
		sessionDao.deleteSession(uuid);
	}

	@Override
	public List<Session> getStaleSessions(long maxLastSeen) {
		return sessionDao.getStaleSessions(maxLastSeen);
	}
}
