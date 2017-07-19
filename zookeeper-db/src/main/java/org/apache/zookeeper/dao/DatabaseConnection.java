package org.apache.zookeeper.dao;

import org.sqlite.SQLiteConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Created by natalia on 7/17/17.
 */
public class DatabaseConnection {

	static {
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (Exception e) {
			System.err.println(e.getClass().getName() + ": " + e.getMessage());
			System.exit(0);
		}
	}

	private String db;
	private SQLiteConfig config;

	public DatabaseConnection(String db) {
		this.db = db;

		config = new SQLiteConfig();
		config.setBusyTimeout("1000");
	}

	protected Connection getConnection() throws SQLException {
		return DriverManager.getConnection("jdbc:sqlite:" + db, config.toProperties());
	}

}
