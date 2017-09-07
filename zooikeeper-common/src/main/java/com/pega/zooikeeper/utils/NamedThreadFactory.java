package com.pega.zooikeeper.utils;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by natalia on 7/18/17.
 */
public class NamedThreadFactory implements ThreadFactory {

	private final AtomicInteger threadNumber = new AtomicInteger(1);
	private String prefix;

	public NamedThreadFactory(String name) {
		this.prefix = name+"-";
	}

	@Override
	public Thread newThread(Runnable r) {
		Thread t = new Thread(r);
		t.setDaemon(true);
		t.setName(prefix+threadNumber.getAndIncrement());
		return t;
	}
}
