package org.apache.zookeeper.impl.broker.service;

import org.apache.zookeeper.impl.broker.bean.BrokerInfo;
import org.apache.zookeeper.impl.broker.dao.BrokerDao;
import org.apache.zookeeper.impl.common.NamedThreadFactory;
import org.apache.zookeeper.impl.node.service.NodeService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by natalia on 7/24/17.
 */
public class BrokerMonitorService {

	private final static Logger logger = LoggerFactory.getLogger(BrokerMonitorService.class.getName());

	private final ScheduledExecutorService brokerInfoUpdater;
	private final ScheduledExecutorService brokersChecker;
	private final long sessionTimeout;
	private final int brokerId;
	private final long session;
	private final BrokerDao brokerDao;
	private final NodeService nodeService;

	private long lastSeen;

	public BrokerMonitorService(BrokerDao brokerDao, NodeService nodeService, int brokerId, long session, long sessionTimeout) {
		this.brokerDao = brokerDao;
		this.sessionTimeout = sessionTimeout;
		this.brokerId = brokerId;
		this.session = session;
		this.nodeService = nodeService;

		this.brokerInfoUpdater = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("BrokerMonitor"));
		this.brokersChecker = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("inactive-broker-checker"));
	}

	private void invalidateStaleBrokers() {
		try {
			List<BrokerInfo> staleBrokers = brokerDao.getBrokersInfo(System.currentTimeMillis() - sessionTimeout * 3);

			for (BrokerInfo brokerInfo : staleBrokers) {
				logger.info(String.format("Found stale broker %d session %d, invalidating the session", brokerInfo.getBrokerId(), brokerInfo.getSession()));

				nodeService.removeSessionNodes(brokerInfo.getSession());

				// Delete broker session info only after session ephemeral nodes are removed.
				brokerDao.delete(brokerInfo);
			}
		} catch (Exception e) {
			logger.warn("Failed to invalidate stale brokers", e);
		}
	}

	public void start() {
		// Stale brokers should be invalidated during startup. This is important in case one of the stale brokers is current broker with previous session.
		invalidateStaleBrokers();
		brokerInfoUpdater.scheduleAtFixedRate(() -> updateBrokerInfo(), sessionTimeout / 2, sessionTimeout / 2, TimeUnit.MILLISECONDS);
		brokersChecker.scheduleAtFixedRate(() -> invalidateStaleBrokers(), sessionTimeout, sessionTimeout, TimeUnit.MILLISECONDS);
	}

	private void updateBrokerInfo() {
		try {
			lastSeen = System.currentTimeMillis();
			brokerDao.update(new BrokerInfo(brokerId, session, lastSeen));
		} catch (Exception e) {
			logger.warn("Failed to update broker info");
		}
	}

	public void stop() {
		try {
			brokerInfoUpdater.shutdown();
		} catch (Exception e) {
		}

		try {
			brokersChecker.shutdown();
		} catch (Exception e) {
		}

		try {
			brokerDao.delete(new BrokerInfo(brokerId, session, lastSeen));
		} catch (Exception e) {
		}
	}
}
