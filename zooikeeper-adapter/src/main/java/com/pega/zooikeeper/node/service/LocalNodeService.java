package com.pega.zooikeeper.node.service;

import com.pega.zooikeeper.node.dao.NodeDao;
import com.pega.zooikeeper.watches.service.RemoteNodeUpdateManager;
import com.pega.zooikeeper.watches.service.WatchService;
import org.apache.zookeeper.WatchedEvent;

import java.io.IOException;

public class LocalNodeService extends NodeServiceImpl {

	private final RemoteNodeUpdateManager remoteNodeUpdateManager;

	public LocalNodeService(NodeDao nodeDao, RemoteNodeUpdateManager remoteNodeUpdateManager, WatchService watchService) throws IOException {

		super(nodeDao, watchService);

		this.remoteNodeUpdateManager = remoteNodeUpdateManager;
		this.remoteNodeUpdateManager.addNodeUpdateListener(watchesNotifier);
		this.remoteNodeUpdateManager.start();
	}

	@Override
	protected void processEvent(WatchedEvent event) {
		super.processEvent(event);
		if (event.getPath() != null) {
			remoteNodeUpdateManager.processLocalWatchedEvent(event);
		}
	}
}
