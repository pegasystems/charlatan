package org.apache.zookeeper.impl.watches.service;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.impl.common.NamedThreadFactory;
import org.apache.zookeeper.impl.node.bean.NodeUpdate;
import org.apache.zookeeper.impl.watches.dao.NodeUpdateDao;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Simple implementation of RemotedNodeUpdate.
 * <p>
 * Every new local WatchedEvent is stored in the database.
 * WatchedEvents generated by remote brokers are pulled from the database every half a second.
 */
public class RemoteNodeUpdateManagerImpl extends RemoteNodeUpdateManager {

	private static Logger logger = LoggerFactory.getLogger(RemoteNodeUpdateManagerImpl.class);
	protected Set<NodeUpdate> lastCheckedNodeUpdates;
	protected long lastCheckedTimestamp;
	private NodeUpdateDao nodeUpdateDao;
	private int broker;
	private ScheduledExecutorService cleanerService;
	private ScheduledExecutorService updatesPullService;

	public RemoteNodeUpdateManagerImpl(NodeUpdateDao nodeUpdateDao) {
		this(nodeUpdateDao, System.currentTimeMillis(), 100 + (int) (Math.random() * 100));
	}

	public RemoteNodeUpdateManagerImpl(NodeUpdateDao nodeUpdateDao, long serviceStartTime, int brokerId) {
		this.nodeUpdateDao = nodeUpdateDao;
		this.broker = brokerId;

		this.lastCheckedTimestamp = serviceStartTime;
		this.lastCheckedNodeUpdates = new HashSet<>();

		cleanerService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Updates-cleaner"));
		updatesPullService = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("Pull-updates"));
	}

	public void start() {
		cleanerService.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				clearUpdates();
			}
		}, 1, 1, TimeUnit.MINUTES);

		updatesPullService.scheduleWithFixedDelay(new Runnable() {
			@Override
			public void run() {
				pullUpdates();
			}
		}, 5, 1000, TimeUnit.MILLISECONDS);
	}

	protected void pullUpdates() {
		try {
			List<NodeUpdate> updates = nodeUpdateDao.getNodeUpdates(broker, lastCheckedTimestamp);

			Set<NodeUpdate> latestNodeUpdates = new HashSet<>(lastCheckedNodeUpdates);
			long latestTimestamp = lastCheckedTimestamp;

			for (NodeUpdate update : updates) {
				if (!latestNodeUpdates.contains(update)) {
					logger.info("Processing update " + update);
					WatchedEvent event = new WatchedEvent(update.getEventType(), Watcher.Event.KeeperState.SyncConnected, update.getPath());
					processRemoteWatchedEvent(event);

					if (update.getTimestamp() > latestTimestamp) {
						latestTimestamp = update.getTimestamp();
						latestNodeUpdates.clear();
						latestNodeUpdates.add(update);
					} else if (update.getTimestamp() == latestTimestamp) {
						latestNodeUpdates.add(update);
					}
				}
			}

			if (latestTimestamp == lastCheckedTimestamp) {
				// Small optimization: if twice in a row we got same updates with the same timestamp we can assume that next updates will have higher timestamp.
				if (!lastCheckedNodeUpdates.isEmpty()) {
					lastCheckedTimestamp++;
					lastCheckedNodeUpdates.clear();
				}
			} else if (latestTimestamp > lastCheckedTimestamp) {
				lastCheckedTimestamp = latestTimestamp;
				lastCheckedNodeUpdates.clear();
				lastCheckedNodeUpdates.addAll(latestNodeUpdates);
			} else {
				logger.warn(String.format("Node updates have timestamp %d, that is smaller then requested %d", latestTimestamp, lastCheckedTimestamp));
			}
		} catch (Throwable e) {
			logger.warn("Pull updates failed [" + e.getMessage() + "]");
		}
	}

	protected void clearUpdates() {
		try {
			nodeUpdateDao.clearOldUpdates(Math.min(System.currentTimeMillis() - 5 * 60 * 1000, lastCheckedTimestamp));
		} catch (Throwable e) {
			logger.warn("clear updates failed [" + e.getMessage() + "]");
		}
	}

	@Override
	public void processLocalWatchedEvent(WatchedEvent event) {
		try {
			NodeUpdate update = new NodeUpdate(event.getType(), event.getPath(), System.currentTimeMillis(), broker);
			nodeUpdateDao.insertUpdate(update);
		} catch (Throwable e) {
			logger.warn("clear updates failed [" + e.getMessage() + "]");
		}
	}
}
