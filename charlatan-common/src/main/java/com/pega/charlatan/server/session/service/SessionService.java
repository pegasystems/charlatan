package com.pega.charlatan.server.session.service;

import com.pega.charlatan.server.session.bean.Session;

import java.util.List;
import java.util.UUID;

public interface SessionService {
	void registerSession(String serverId, Session session);

	void updateSession(Session session);

	void deleteSession(UUID uuid);

	List<Session> getStaleSessions(long maxLastSeen);
}
