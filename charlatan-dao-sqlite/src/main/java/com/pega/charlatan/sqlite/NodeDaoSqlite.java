package com.pega.charlatan.sqlite;

import com.pega.charlatan.node.dao.DataAccessException;
import com.pega.charlatan.node.dao.NodeDao;
import com.pega.charlatan.node.dao.RecordNotFoundException;
import com.pega.charlatan.utils.Service;
import com.pega.charlatan.node.bean.Node;

import com.pega.charlatan.node.bean.CreateMode;
import com.pega.charlatan.node.bean.NodeState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by natalia on 7/11/17.
 */
@Service
public class NodeDaoSqlite extends DatabaseConnection implements NodeDao {
	private static Logger logger = LoggerFactory.getLogger(NodeDaoSqlite.class.getName());

	@Override
	public boolean create(long session, Node node) throws RecordNotFoundException {
		StringBuilder sql = new StringBuilder("INSERT INTO nodes(fk, name, data, create_time, modify_time, session");
		if (node.getMode() != null)
			sql.append(",mode");
		sql.append(" ) VALUES(?,?,?,?,?,?");
		if (node.getMode() != null)
			sql.append(",?");
		sql.append(" )");

		int parentKey = getNodeKey(node.getParentPath());

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c, sql.toString())) {
				long now = System.currentTimeMillis();

				ps.setInt(1, parentKey);
				ps.setString(2, node.getPath());
				ps.setBytes(3, node.getData());
				ps.setLong(4, now);
				ps.setLong(5, now);
				ps.setLong(6, session);

				if (node.getMode() != null)
					ps.setString(7, node.getMode().name());

				return executeUpdate(ps) > 0;
			}
		} catch (SQLException e) {
			if (e.getErrorCode() == 19) {//constraint violation
				return false;
			}
			throw new DataAccessException(e);
		}
	}


	@Override
	public boolean delete(Node node) {
		String sql = "DELETE FROM nodes WHERE name=?";

		boolean hasVersion = node.getState().getVersion() >= 0;

		if (hasVersion)
			sql += "and version=?";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c,sql)) {
				ps.setString(1, node.getPath());
				if (hasVersion) {
					ps.setInt(2, node.getState().getVersion());
				}
				return executeUpdate(ps) > 0;
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}
	}

	@Override
	public Node get(String path) throws RecordNotFoundException {

		String getNodeData = "SELECT pk, data, version, cversion, mode, create_time, modify_time, session FROM nodes WHERE name = ? ";
		String getNodeChildren = "SELECT name FROM nodes WHERE fk=?";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c,getNodeData)) {
				ps.setString(1, path);
				ResultSet rs = executeQuery(ps);

				if (rs.next()) {
					Node node = new Node(path);

					byte[] data = rs.getBytes("data");
					node.setData(data);
					NodeState stat = node.getState();
					stat.setVersion(rs.getInt("version"));
					stat.setCversion(rs.getInt("cversion"));
					stat.setCtime(rs.getLong("create_time"));
					stat.setMtime(rs.getLong("modify_time"));
					if (data != null) {
						stat.setDataLength(data.length);
					}
					node.setMode(CreateMode.valueOf(rs.getString("mode")));
					if (node.getMode().isEphemeral()) {
						stat.setEphemeralOwner(rs.getLong("session"));
					}

					int pk = rs.getInt("pk");
					rs.close();

					try (PreparedStatement ps2 = prepareStatement(c,getNodeChildren)) {
						ps2.setInt(1, pk);
						rs = executeQuery(ps2);
						List<String> children = new ArrayList<>();
						while (rs.next()) {
							String fullPath = rs.getString("name");

							int startIndex;
							if(path.equals("/")){
								startIndex = 1;
							}
							else {
								startIndex = node.getPath().length() + 1;
							}

							String nodeName = fullPath.substring(startIndex);
							children.add(nodeName);
						}
						node.setChildren(children);
						stat.setNumChildren(children.size());
					}

					return node;
				}
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}

		throw new RecordNotFoundException(path);
	}

	@Override
	public void update(String path, byte[] data, int newVersion, long modificationTime) {
		String sql = "UPDATE nodes SET data=?, version = ?, modify_time=? WHERE name=?";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c,sql)) {
				ps.setBytes(1, data);
				ps.setInt(2, newVersion);
				ps.setLong(3, modificationTime);
				ps.setString(4, path);

				executeUpdate(ps);
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}
	}

	@Override
	public void updateCVersion(String path, int cversion) {
		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c,"UPDATE nodes SET cversion=? WHERE name=?")) {
				ps.setInt(1, cversion);
				ps.setString(2, path);
				executeUpdate(ps);
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}
	}

	@Override
	public List<String> getEphemeralPaths(long session) {
		String sql = "SELECT name FROM nodes WHERE session=? AND mode='EPHEMERAL'";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c,sql)) {
				ps.setLong(1, session);
				ResultSet rs = executeQuery(ps);
				List<String> paths = new ArrayList<>();
				while (rs.next()) {
					paths.add(rs.getString("name"));

				}
				return paths;
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}
	}

	protected int getNodeKey(String path) throws RecordNotFoundException {

		String sql = "SELECT pk FROM nodes WHERE name = ? ";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c,sql)) {
				ps.setString(1, path);
				ResultSet rs = executeQuery(ps);
				if (rs.next()) {
					return rs.getInt("pk");
				}
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}

		throw new RecordNotFoundException(path + " not found");
	}

	@Override
	protected void setup() {
		String createTable = "CREATE TABLE IF NOT EXISTS `nodes` (  `pk` integer PRIMARY KEY, `fk` integer NULL,  \n" +
				"`name` text NOT NULL, \n" +
				"`data` BLOB,\n" +
				"`version` integer NOT NULL DEFAULT 0,\n" +
				"`cversion` integer NOT NULL DEFAULT 0,\n" +
				"`create_time`  long NOT NULL,\n" +
				"`modify_time`  long NOT NULL,\n" +
				"`mode` text NOT NULL default 'PERSISTENT', \n" +
				"`session` integer,\n" +
				"FOREIGN KEY (fk) REFERENCES nodes(pk), UNIQUE(name) )";

		String insertRoot = "INSERT INTO nodes (name,create_time, modify_time) VALUES (\"/\",?,?);";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = prepareStatement(c,createTable)) {
				executeUpdate(ps);
			}
			try (PreparedStatement ps = prepareStatement(c,insertRoot)) {
				long now = System.currentTimeMillis();
				ps.setLong(1, now);
				ps.setLong(2, now);
				executeUpdate(ps);
			}
		} catch (Exception e) {
			//
		}
	}
}
