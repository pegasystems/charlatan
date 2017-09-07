//package com.pega.zooikeeper.node.service;
//
//import com.pega.zooikeeper.node.dao.NodeDao;
//import com.pega.zooikeeper.watches.service.WatchService;
//import com.pega.zooikeeper.watches.service.WatchCache;
//import org.apache.zookeeper.WatchedEvent;
//
//import java.io.IOException;
//
//public class LocalNodeService extends NodeServiceImpl {
//
//	private final WatchService watchService;
//
//	public LocalNodeService(NodeDao nodeDao, WatchService watchService, WatchCache watchCache) throws IOException {
//
//		super(nodeDao, watchCache);
//
//		this.watchService = watchService;
//		this.watchService.addNodeUpdateListener(watchesNotifier);
//		this.watchService.start();
//	}
//
//	@Override
//	protected void processEvent(WatchedEvent event) {
//		super.processEvent(event);
//		if (event.getPath() != null) {
//			watchService.processLocalWatchedEvent(event);
//		}
//	}
//}
