package org.apache.zookeeper.impl.sqlite;

import org.apache.zookeeper.impl.node.dao.DataAccessException;
import org.sqlite.SQLiteConfig;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

/**
 * Created by natalia on 7/17/17.
 */
public class DatabaseConnection {

	private static long retryTimeoutInMillis = 60000;
	private static String database;
	private static SQLiteConfig config = new SQLiteConfig();

	static {
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}

		readProperties();
	}

	private static void readProperties() {
		Properties prop = new Properties();
		InputStream input = null;

		try {

			input = new FileInputStream("sqlite.properties");

			prop.load(input);

			database = prop.getProperty("database");
		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	private static void initConfig() {
		config = new SQLiteConfig();
		config.setBusyTimeout("1000");
	}

	protected Connection getConnection() throws SQLException {
		return retryUntilSucceeded(() -> {
			return DriverManager.getConnection("jdbc:sqlite:" + database, config.toProperties());
		});
	}

	protected PreparedStatement prepareStatement(Connection c, String sql ) throws SQLException {
		return retryUntilSucceeded(() -> {
			return c.prepareStatement(sql);
		});
	}

	protected int executeUpdate(PreparedStatement ps) throws SQLException {
		return retryUntilSucceeded(() -> {
			return ps.executeUpdate();
		});
	}

	protected ResultSet executeQuery(PreparedStatement ps) throws SQLException {
		return retryUntilSucceeded(() -> {
			return ps.executeQuery();
		});
	}

	public <T> T retryUntilSucceeded(Callable<T> callable) throws SQLException {
		final long operationStartTime = System.currentTimeMillis();
		SQLException lastEx;
		while (true) {
			try {
				return callable.call();
			} catch (SQLException e) {
				if (e.getErrorCode() == 5) {
					// we give the event thread some time to update the status to 'Disconnected'
					lastEx = e;
					try {
						Thread.sleep(500);
					} catch (InterruptedException e1) {
						throw e;
					}
				} else {
					throw e;
				}
			} catch (Exception e) {
				throw new DataAccessException("Error", e);
			}
			// before attempting a retry, check whether retry timeout has elapsed
			if (retryTimeoutInMillis > -1 && (System.currentTimeMillis() - operationStartTime) >= retryTimeoutInMillis) {
				throw lastEx;
			}
		}
	}
}
