package com.pega.zooikeeper.server.session.dao;

import com.pega.zooikeeper.node.dao.RecordNotFoundException;
import com.pega.zooikeeper.server.session.bean.Session;

import java.util.List;
import java.util.UUID;

public interface SessionDao {
	/**
	 * Add session information on specific serverId.
	 *
	 * @param serverId
	 * @param session
	 */
	void insertSesssion(String serverId, Session session);

	/**
	 * Update session details.
	 *
	 * @param session
	 */
	void updateSession(Session session);

	/**
	 * Delete session by UUID.
	 *
	 * @param uuid
	 */
	void deleteSesssion(UUID uuid);

	/**
	 * Return list of the stale sessions.
	 *
	 * @param maxLastSeenTime
	 * @return
	 */
	List<Session> getStaleSessions(long maxLastSeenTime);

	Session getSession(UUID uuid) throws RecordNotFoundException;
}
