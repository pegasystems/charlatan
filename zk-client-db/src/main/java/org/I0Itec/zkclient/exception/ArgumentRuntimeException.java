package org.I0Itec.zkclient.exception;

/**
 * Created by natalia on 7/11/17.
 */
public class ArgumentRuntimeException extends RuntimeException {
	public ArgumentRuntimeException(String message) {
		super(message);
	}

	public ArgumentRuntimeException(Exception e) {
		super(e);
	}
}
