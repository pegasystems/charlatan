package com.pega.zooikeeper.broker.service;

import com.pega.zooikeeper.broker.bean.BrokerInfo;
import com.pega.zooikeeper.broker.dao.BrokerDao;
import com.pega.zooikeeper.node.service.NodeService;
import com.pega.zooikeeper.utils.NamedThreadFactory;
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
	private final BrokerDao brokerDao;
	private final NodeService nodeService;
	private int brokerId;
	private long session;
	private long lastSeen;
	private State state;

	public BrokerMonitorService(BrokerDao brokerDao, NodeService nodeService, long sessionTimeout) {
		this.brokerDao = brokerDao;
		this.sessionTimeout = sessionTimeout;
		this.nodeService = nodeService;

		this.brokerInfoUpdater = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("BrokerMonitor"));
		this.brokersChecker = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("inactive-broker-checker"));

		setState(State.CREATED);
	}

	private void invalidateStaleBrokers(boolean includeThisBroker) {
		try {
			List<BrokerInfo> staleBrokers = brokerDao.getBrokersInfo(System.currentTimeMillis() - sessionTimeout * 3);

			for (BrokerInfo brokerInfo : staleBrokers) {

				if(!includeThisBroker && this.brokerId == brokerInfo.getBrokerId()){
					logger.warn("This broker is in list of stale brokers!");
				} else {

					logger.info(String.format("Found stale broker %d session %d, invalidating the session", brokerInfo.getBrokerId(), brokerInfo.getSession()));

					nodeService.removeEphemeralSessionNodes(brokerInfo.getSession());

					// Delete broker session info only after session ephemeral nodes are removed.
					brokerDao.delete(brokerInfo);
				}
			}
		} catch (Throwable e) {
			logger.warn("Failed to invalidate stale brokers", e);
		}
	}

	public synchronized void start(int brokerId, long session) {
		checkState(State.CREATED);
		this.brokerId = brokerId;
		this.session = session;
		// Stale brokers should be invalidated during startup. This is important in case one of the stale brokers is current broker with previous session.
		invalidateStaleBrokers(true);
		brokerInfoUpdater.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				updateBrokerInfo();
			}
		}, sessionTimeout / 2, sessionTimeout / 2, TimeUnit.MILLISECONDS);
		brokersChecker.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				invalidateStaleBrokers(false);
			}
		}, sessionTimeout, sessionTimeout, TimeUnit.MILLISECONDS);
		setState(State.STARTED);
	}

	private void updateBrokerInfo() {
		try {
			lastSeen = System.currentTimeMillis();
			brokerDao.update(new BrokerInfo(brokerId, session, lastSeen));
		} catch (Throwable e) {
			logger.warn("Failed to update broker info");
		}
	}

	public synchronized void stop() {
		if (state == State.STARTED) {
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
			setState(State.STOPPED);
		}
	}

	private void checkState(State state) {
		if (this.state != state) {
			throw new IllegalStateException(String.format("Expected state is %s but current state is %s", state, this.state));
		}
	}

	private void setState(State state) {
		this.state = state;

	}

	public boolean isStarted() {
		return this.state == State.STARTED;
	}

	enum State {
		CREATED,
		STARTED,
		STOPPED
	}
}
