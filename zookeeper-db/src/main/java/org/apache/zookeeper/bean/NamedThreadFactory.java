package org.apache.zookeeper.bean;

import java.util.concurrent.ThreadFactory;

/**
 * Created by natalia on 7/18/17.
 */
public class NamedThreadFactory implements ThreadFactory {

	private String name;

	public NamedThreadFactory(String name) {
		this.name = name;
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread t = new Thread(r);
		t.setName(name);
		return t;
	}
}
