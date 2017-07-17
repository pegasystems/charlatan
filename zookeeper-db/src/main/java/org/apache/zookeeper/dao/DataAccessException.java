package org.apache.zookeeper.dao;

import java.sql.SQLException;

/**
 * Created by natalia on 7/11/17.
 */
public class DataAccessException extends RuntimeException {
	public DataAccessException(SQLException e) {
		super(e);
	}

	public DataAccessException(String msg) {
		super(msg);
	}
}
