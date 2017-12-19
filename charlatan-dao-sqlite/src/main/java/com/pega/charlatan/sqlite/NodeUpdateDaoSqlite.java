package com.pega.charlatan.sqlite;

import com.pega.charlatan.node.bean.NodeUpdate;
import com.pega.charlatan.node.dao.DataAccessException;
import com.pega.charlatan.watches.dao.NodeUpdateDao;
import com.pega.charlatan.watches.bean.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by natalia on 7/17/17.
 */
public class NodeUpdateDaoSqlite extends DatabaseConnection implements NodeUpdateDao {

	private final static Logger logger = LoggerFactory.getLogger(NodeUpdateDaoSqlite.class.getName());

	@Override
	public void insertUpdate(NodeUpdate update) {
		String sql = "INSERT INTO node_updates(type,path,broker,timestamp) VALUES (?,?,?,?)";
		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c, sql)) {

				ps.setString(1, update.getType().name());
				ps.setString(2, update.getPath());
				ps.setInt(3, update.getEmitterId());
				ps.setLong(4, System.currentTimeMillis());

				executeUpdate(ps);
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}
	}


	@Override
	public List<NodeUpdate> getNodeUpdates(int thisBroker, long fromTimestamp) {
		List<NodeUpdate> updates = new ArrayList<>();

		String sql = "SELECT type, path, timestamp, broker FROM node_updates " +
				"WHERE " +
				"timestamp >= ?  " +
				"AND broker != ?";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c, sql)) {

				int i = 1;
				ps.setLong(i++, fromTimestamp);
				ps.setInt(i++, thisBroker);

				ResultSet rs = executeQuery(ps);
				while (rs.next()) {
					Watcher.Event.Type type = Watcher.Event.Type.valueOf(rs.getString("type"));
					String path = rs.getString("path");
					long timestamp = rs.getLong("timestamp");
					int broker = rs.getInt("broker");

					updates.add(new NodeUpdate(type, path, timestamp, broker));
				}
			}
		} catch (SQLException e) {
			logger.error("pull updates error", e);
			throw new DataAccessException(e);
		}
		return updates;
	}


//	@Override
//	public void clearProcessedUpdates(int ownerBroker, int toId) {
//		String sql = "DELETE FROM node_updates WHERE  broker=? and id<=?";
//
//		try (Connection c = getConnection()) {
//			try (PreparedStatement ps = c.prepareStatement(sql)) {
//
//				ps.setInt(1, ownerBroker);
//				ps.setInt(2, toId);
//
//				ps.executeUpdate();
//			}
//		} catch (SQLException e) {
//			throw new DataAccessException(e);
//		}
//	}

	@Override
	public void clearOldUpdates(long toMs) {
		String sql = "DELETE FROM node_updates WHERE timestamp<?";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = c.prepareStatement(sql)) {
				ps.setLong(1, toMs);

				ps.executeUpdate();
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}
	}

	@Override
	protected void setup() {
		String createTable = "CREATE TABLE IF NOT EXISTS `node_updates` ( `id` integer PRIMARY KEY,   \n" +
				"`type` text NOT NULL, \n" +
				"`path`,\n" +
				"`broker` integer,\n" +
				"`timestamp`  long NOT NULL)";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c, createTable)) {
				executeUpdate(ps);
			}
		} catch (Exception e) {
			//
		}
	}
}
