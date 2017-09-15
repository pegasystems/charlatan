//package com.pega.zooikeeper.server.session.dao;
//
//import com.pega.zooikeeper.broker.bean.BrokerInfo;
//import com.pega.zooikeeper.node.dao.DataAccessException;
//import com.pega.zooikeeper.node.dao.RecordNotFoundException;
//import com.pega.zooikeeper.server.session.bean.Session;
//import com.pega.zooikeeper.sqlite.DatabaseConnection;
//
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.UUID;
//
//public class SessionDaoSqlite extends DatabaseConnection implements SessionDao{
//	@Override
//	public void insertSesssion(String serverId, Session session) {
//		String sql = "INSERT INTO sessions(uuid,id,start_time,last_seen,server_id) VALUES(?,?,?,?,?)";
//
//		try (Connection c = getConnection()) {
//			try (PreparedStatement ps = prepareStatement(c, sql)) {
//				ps.setString(1, session.getUuid().toString());
//				ps.setLong(2, session.getSessionId());
//				ps.setLong(3, session.getStartTime());
//				ps.setLong(4, session.getLastTimeSeen());
//				ps.setString(5, serverId);
//
//				executeUpdate(ps);
//			}
//		} catch (SQLException e) {
//			throw new DataAccessException(e);
//		}
//	}
//
//	@Override
//	public void updateSession(Session session) {
//		String sql = "UPDATE sessions SET id=?,last_seen=?,timeout=? WHERE uuid=?";
//
//		try (Connection c = getConnection()) {
//			try (PreparedStatement ps = prepareStatement(c, sql)) {
//				ps.setLong(1, session.getSessionId());
//				ps.setLong(2, session.getLastTimeSeen());
//				ps.setInt(3, session.getTimeout());
//				ps.setString(4, session.getUuid().toString());
//
//				executeUpdate(ps);
//			}
//		} catch (SQLException e) {
//			throw new DataAccessException(e);
//		}
//	}
//
//	@Override
//	public void deleteSesssion(UUID uuid) {
//		String sql = "DELETE FROM sessions WHERE uuid=?";
//
//		try (Connection c = getConnection()) {
//			try (PreparedStatement ps = prepareStatement(c, sql)) {
//				ps.setString(1, uuid.toString());
//
//				executeUpdate(ps);
//			}
//		} catch (SQLException e) {
//			throw new DataAccessException(e);
//		}
//	}
//
//	@Override
//	public List<Session> getStaleSessions(long maxLastSeenTime) {
//		String sql = "SELECT uuid, id, timeout, last_seen, start_time FROM sessions WHERE last_seen < ?";
//
//		try (Connection c = getConnection()) {
//			try (PreparedStatement ps = prepareStatement(c, sql)) {
//				ps.setLong(1, maxLastSeenTime);
//
//				ResultSet rs = executeQuery(ps);
//
//				List<Session> sessions = new ArrayList<>();
//				while (rs.next()) {
//					UUID uuid = UUID.fromString(rs.getString("uuid"));
//					long id = rs.getLong("id");
//					long startTime = rs.getLong("start_time");
//					long lastSeen = rs.getLong("last_seen");
//					int timeout = rs.getInt("timeout");
//
//					Session session = new Session(uuid, startTime);
//					session.setTimeout(timeout);
//					session.setLastTimeSeen(lastSeen);
//					session.setSessionId(id);
//
//					sessions.add(session);
//				}
//				return sessions;
//			}
//		} catch (SQLException e) {
//			throw new DataAccessException(e);
//		}
//	}
//
//	@Override
//	public Session getSession(UUID uuid) throws RecordNotFoundException {
//		return null;
//	}
//
//	@Override
//	protected void setup() {
//		String createTable = "CREATE TABLE IF NOT EXISTS `sessions` (  \n" +
//				"`uuid` text NOT NULL  PRIMARY KEY," +
//				"`id` integer, \n" +
//				"`timeout` integer NULL,  \n" +
//				"`start_time` long NULL,  \n" +
//				"`last_seen` long NULL, " +
//				"`server_id` text NULL);";
//
//		try (Connection c = getConnection()) {
//			try (PreparedStatement ps = prepareStatement(c, createTable)) {
//				executeUpdate(ps);
//			}
//		} catch (Exception e) {
//			//
//		}
//	}
//}
