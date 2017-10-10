package com.pega.charlatan.server;

import java.util.HashMap;
import java.util.Map;

public enum RequestType {
	Connect(0),
	Create(1),
	Delete(2),
	Exists(3),
	GetData(4),
	SetData(5),
	GetAcl(6),
	SetAcl(7),
	GetChildren(8),
	Ping(11),
	GetChildren2(12),
	Create2(15),
	SetWatches(101),
	CloseSession(-11);

	private static Map<Integer, RequestType> requestMap = new HashMap<>();

	static {
		for (RequestType r : values()) {
			requestMap.put(r.getCode(), r);
		}
	}

	private int code;

	RequestType(int code) {
		this.code = code;
	}

	public static RequestType getRequestType(int code) {
		return requestMap.get(code);
	}

	public int getCode() {
		return code;
	}
}