package com.pega.charlatan.node.dao;

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

	public DataAccessException(String error, Exception e) {
		super(error, e);
	}
}
