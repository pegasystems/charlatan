package com.pega.zooikeeper.common;

/**
 * Created by natalia on 7/10/17.
 */
public class FakeZookeeperException extends RuntimeException {

	private Code code;

	public FakeZookeeperException(String ex) {
		super(ex);
	}

	public FakeZookeeperException(Code code) {
		this.code = code;
	}

	public Code getCode() {
		return code;
	}


	public static enum Code{
		UNIMPLEMENTED;
	}
}
