package org.apache.zookeeper.dao;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.dao.bean.Node;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by natalia on 7/11/17.
 */
public class NodeDaoImpl implements NodeDao {

	static {
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
	}

	private String db;

	public NodeDaoImpl(String db) {
		this.db = db;
	}

	private Connection getConnection() throws SQLException {
		return DriverManager.getConnection("jdbc:sqlite:" + db);
	}

	@Override
	public boolean exists(Node node) {
		try {
			return getNodeKey(node) >= 0;
		} catch (RecordNotFoundException e) {
			return false;
		}
	}

	@Override
	public boolean create(Node node) throws RecordNotFoundException {
		Integer parentKey = null;
		if (!node.isRoot()) {
			parentKey = getNodeKey(node.getParent());
		}

		StringBuilder sql = new StringBuilder( "INSERT INTO nodes(fk, name, data, timestamp" );
		if( node.getMode() != null )
			sql.append(",mode");
		sql.append(" ) VALUES(?,?,?,?");
		if( node.getMode() != null )
			sql.append(",?");
		sql.append(" )");

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = c.prepareStatement(sql.toString())) {
				ps.setInt(1, parentKey);
				ps.setString(2, node.getPath());
				ps.setBytes(3, node.getData());
				ps.setLong(4, System.currentTimeMillis());

				if( node.getMode() != null )
				   	ps.setString(5, node.getMode().name());

				return ps.executeUpdate() > 0;
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
		if (node.hasVersion())
			sql += "and version=?";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = c.prepareStatement(sql)) {
				ps.setString(1, node.getPath());
				if (node.hasVersion()) {
					ps.setInt(2, node.getVersion());
				}
				return ps.executeUpdate() > 0;
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}

	}

	@Override
	public Node get(String path) throws RecordNotFoundException {
		String sql = "SELECT name, data, version, mode, timestamp FROM nodes WHERE name = ? ";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = c.prepareStatement(sql)) {
				ps.setString(1, path);
				ResultSet rs = ps.executeQuery();
				if (rs.next()) {
					byte[] data = rs.getBytes("data");
					int version = rs.getInt("version");
					long timestamp = rs.getLong("timestamp");
					CreateMode mode= CreateMode.valueOf( rs.getString( "mode") );

					return new Node(path, data, version, timestamp, mode );
				}
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}

		throw new RecordNotFoundException(path + " not found");
	}

	@Override
	public void update(Node node) throws RecordNotFoundException {
		String sql = "UPDATE nodes SET data=?, version = version+1 WHERE name=? and version=?";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = c.prepareStatement(sql)) {
				ps.setBytes(1, node.getData());
				ps.setString(2, node.getPath());
				ps.setInt(3, node.getVersion());
				if (ps.executeUpdate() <= 0) {
					throw new RecordNotFoundException(node.getPath() + " not found");
				}
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}
	}

	@Override
	public List<String> getChildren(Node node) throws RecordNotFoundException {
		String sql = "SELECT name FROM nodes WHERE fk=?";

		int key = getNodeKey(node);

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = c.prepareStatement(sql)) {
				ps.setInt(1, key);
				ResultSet rs = ps.executeQuery();
				List<String> children = new ArrayList<>();
				while (rs.next()) {
					children.add(rs.getString("name"));
				}
				return children;
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}
	}

	protected int getNodeKey(Node node) throws RecordNotFoundException {
		String sql = "SELECT pk FROM nodes WHERE name = ? ";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = c.prepareStatement(sql)) {
				ps.setString(1, node.getPath());
				ResultSet rs = ps.executeQuery();
				if (rs.next()) {
					return rs.getInt("pk");
				}
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}

		throw new RecordNotFoundException(node.getPath() + " not found");
	}

	protected void setup() {
		String createTable = "CREATE TABLE IF NOT EXISTS `nodes` (  `pk` integer PRIMARY KEY, `fk` integer NULL,  \n" +
				"`name` text NOT NULL, \n" +
				"`data` BLOB,\n" +
				"`version` integer NOT NULL DEFAULT 1,\n" +
				"`timestamp`  long NOT NULL,\n" +
				"`mode` text NOT NULL default 'PERSISTENT', \n" +
				"FOREIGN KEY (fk) REFERENCES nodes(pk) ON DELETE CASCADE, UNIQUE(name) );";

		String insertRoot = "INSERT INTO nodes (name,timestamp) VALUES (\"/\",?)";

		try (Connection c = getConnection()) {
			try (PreparedStatement ps = c.prepareStatement(createTable)) {
				ps.executeUpdate();
			}
			try (PreparedStatement ps = c.prepareStatement(insertRoot)) {
				ps.setLong(1, System.currentTimeMillis() );
				ps.executeUpdate();
			}
		} catch (SQLException e) {
			throw new DataAccessException(e);
		}

	}
}
