package org.apache.zookeeper.impl.sqlite;

import org.apache.zookeeper.impl.broker.bean.BrokerInfo;
import org.apache.zookeeper.impl.broker.dao.BrokerDao;
import org.apache.zookeeper.impl.node.dao.DataAccessException;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by natalia on 7/24/17.
 */
public class BrokerDaoImpl extends DatabaseConnection implements BrokerDao {


	@Override
	public List<BrokerInfo> getBrokersInfo(long maxLastSeen) {
		String sql = "SELECT id, session, last_seen FROM brokers WHERE last_seen < ?";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c, sql)) {
				ps.setLong(1, maxLastSeen);

				ResultSet rs = executeQuery(ps);

				List<BrokerInfo> brokers = new ArrayList<>();
				while (rs.next()) {
					int id = rs.getInt("id");
					long session = rs.getLong("session");
					long lastSeen = rs.getLong("last_seen");

					brokers.add(new BrokerInfo(id, session, lastSeen));
				}
				return brokers;
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}
	}

	@Override
	public boolean delete(BrokerInfo brokerInfo) {
		String sql = "DELETE FROM brokers WHERE id=? AND session=? AND last_seen=?";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c, sql)) {
				ps.setInt(1, brokerInfo.getBrokerId());
				ps.setLong(2, brokerInfo.getSession());
				ps.setLong(3, brokerInfo.getLastTimeSeen());

				return executeUpdate(ps) > 0;
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}
	}

	@Override
	public void update(BrokerInfo brokerInfo) {
		String sql = "INSERT OR REPLACE INTO brokers(id,session,last_seen) VALUES(?,?,?)";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c, sql)) {
				ps.setInt(1, brokerInfo.getBrokerId());
				ps.setLong(2, brokerInfo.getSession());
				ps.setLong(3, brokerInfo.getLastTimeSeen());

				executeUpdate(ps);
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}
	}


	@Override
	protected void setup() {
		String createTable = "CREATE TABLE IF NOT EXISTS `brokers` (  \n" +
				"`id` integer PRIMARY KEY, \n" +
				"`session` integer NULL,  \n" +
				"`last_seen` long NOT NULL);\n";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c, createTable)) {
				executeUpdate(ps);
			}
		} catch (Exception e) {
			//
		}
	}
}
