package org.apache.zookeeper.impl.sqlite;

import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.impl.node.dao.DataAccessException;
import org.apache.zookeeper.impl.node.bean.NodeUpdate;
import org.apache.zookeeper.impl.watches.dao.NodeUpdateDao;
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
	public void insertUpdate(int ownerBroker, NodeUpdate update) {
		String sql = "INSERT INTO node_updates(type,path,broker,timestamp) VALUES (?,?,?,?)";
		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c,sql)) {

				ps.setString(1, update.getEventType().name());
				ps.setString(2, update.getPath());
				ps.setInt(3, ownerBroker);
				ps.setLong(4, System.currentTimeMillis());

				executeUpdate(ps);
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}
	}


	@Override
	public List<NodeUpdate> getNodeUpdates(int ownerBroker, int fromId) {
		List<NodeUpdate> updates = new ArrayList<>();

		String sql = "SELECT id, type, path, timestamp FROM node_updates WHERE id > ? AND broker != ?";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c,sql)) {

				ps.setInt(1, fromId);
				ps.setInt(2, ownerBroker);

				ResultSet rs = executeQuery(ps);
				while (rs.next()) {
					int id = rs.getInt("id");
					Watcher.Event.EventType type = Watcher.Event.EventType.valueOf(rs.getString("type"));
					String path = rs.getString("path");
					long timestamp = rs.getLong("timestamp");

					updates.add(new NodeUpdate(id, type, path, timestamp));
				}
			}
		} catch (SQLException e) {
			logger.error("pull updates error", e);
			throw new DataAccessException(e);
		}
		return updates;

	}

	@Override
	public void clearProcessedUpdates(int ownerBroker, int id) {
		String sql = "DELETE FROM node_updates WHERE  broker=? and id<=?";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = c.prepareStatement(sql)) {

				ps.setInt(1, ownerBroker);
				ps.setInt(2, id);

				ps.executeUpdate();
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}
	}

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
	public int getLastUpdateId() {
		String sql = "SELECT ifnull( MAX(id),-1) as id FROM node_updates";
		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c,sql)) {

				ResultSet rs = executeQuery(ps);
				if (rs.next()) {
					return rs.getInt("id");
				}

				return -1;
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
