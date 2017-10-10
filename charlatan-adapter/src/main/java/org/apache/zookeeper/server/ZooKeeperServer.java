package org.apache.zookeeper.server;

import java.io.File;

/**
 * This class is used only form unit tests
 */
public class ZooKeeperServer {
	public ZooKeeperServer(File snapDir, File logDir, int tickTime) {

	}

	public int getClientPort() {
		return -1;
	}

	public void shutdown() {
	}
}
