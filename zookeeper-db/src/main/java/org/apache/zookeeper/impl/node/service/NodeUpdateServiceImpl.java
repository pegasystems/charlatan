package org.apache.zookeeper.impl.node.service;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.impl.watches.WatchesNotifier;
import org.apache.zookeeper.impl.common.NamedThreadFactory;
import org.apache.zookeeper.impl.node.bean.NodeUpdate;
import org.apache.zookeeper.dao.NodeUpdateDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by natalia on 7/17/17.
 */
public class NodeUpdateServiceImpl implements NodeUpdateService {

	private static Logger logger = LoggerFactory.getLogger(NodeUpdateServiceImpl.class);

	private NodeUpdateDao nodeUpdateDao;
	private int broker;
	private int lastPulledId;
	private ScheduledExecutorService cleanerService;
	private ScheduledExecutorService updatesPullService;
	private WatchesNotifier notifier;
	private boolean lastIdChecked;

	public NodeUpdateServiceImpl(WatchesNotifier notifier, NodeUpdateDao nodeUpdateDao) {
		this.nodeUpdateDao = nodeUpdateDao;
		this.broker = 100 + (int) (Math.random() * 100);
		this.lastPulledId = -1;
		this.notifier = notifier;

		cleanerService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Updates-cleaner"));
		cleanerService.scheduleWithFixedDelay(() -> clearUpdates(), 0, 1, TimeUnit.MINUTES);

		updatesPullService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Pull-updates"));
		updatesPullService.scheduleAtFixedRate(() -> pullUpdates(), 0, 500, TimeUnit.MILLISECONDS);
	}

	@Override
	public void processNodeEvent(WatchedEvent event) {

		if (event.getPath() != null) {
			NodeUpdate nodeUpdate = new NodeUpdate(event.getType(), event.getPath(), System.currentTimeMillis());
			nodeUpdateDao.insertUpdate(broker, nodeUpdate);
		}

		notifier.send(event);
	}

	private void pullUpdates() {
		try {
			if (!lastIdChecked) {
				lastPulledId = getLasttId();
				lastIdChecked = true;
			}

			List<NodeUpdate> updates = nodeUpdateDao.getNodeUpdates(broker, lastPulledId);
			if (!updates.isEmpty()) {
				lastPulledId = updates.get(updates.size() - 1).getId();
			}


			for (NodeUpdate update : updates) {
				logger.info("Processing update " + update);
				WatchedEvent event = new WatchedEvent(update.getEventType(), Watcher.Event.KeeperState.SyncConnected, update.getPath());
				notifier.send(event);
			}
		} catch (Exception e) {
			logger.warn("pull updates failed [" + e.getMessage() +"]");
		}
	}

	private void clearUpdates(){
		try {
			nodeUpdateDao.clearProcessedUpdates(broker, lastPulledId);
			nodeUpdateDao.clearOldUpdates(System.currentTimeMillis() - 10*60*1000);
		}
		catch(Exception e){
			logger.warn("clear updates failed [" + e.getMessage() +"]");
		}

	}

	private int getLasttId() {
		return nodeUpdateDao.getLastUpdateId();
	}
}
