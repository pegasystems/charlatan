package com.pega.charlatan.utils;

/**
 * Created by natalia on 7/19/17.
 */
public class ZookeeperRuntimeException extends RuntimeException {

	public ZookeeperRuntimeException(String message) {
		super(message);
	}

	public ZookeeperRuntimeException(String message, Throwable cause) {
		super(message, cause);
	}
}
