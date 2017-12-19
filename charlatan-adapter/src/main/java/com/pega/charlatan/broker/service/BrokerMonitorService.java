package com.pega.charlatan.broker.service;

import com.pega.charlatan.server.session.bean.Session;
import com.pega.charlatan.server.session.dao.SessionDao;
import com.pega.charlatan.server.session.service.SessionServiceImpl;
import com.pega.charlatan.utils.NamedThreadFactory;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Created by natalia on 7/24/17.
 */
public class BrokerMonitorService extends SessionServiceImpl{

	private final static Logger logger = LoggerFactory.getLogger(BrokerMonitorService.class.getName());

	private final ScheduledExecutorService brokerInfoUpdater;
	private final ScheduledExecutorService brokersChecker;
	private final ZooKeeper zooKeeper;
	private int brokerId;
	private Session session;
	private long lastSeen;
	private State state;

	public BrokerMonitorService(Session session, SessionDao sessionDao, ZooKeeper zooKeeper) {
		super(sessionDao);
		this.zooKeeper = zooKeeper;
		this.session = session;

		this.brokerInfoUpdater = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("BrokerMonitor"));
		this.brokersChecker = Executors.newSingleThreadScheduledExecutor(new NamedThreadFactory("inactive-broker-checker"));

		setState(State.CREATED);
	}

	private void invalidateStaleBrokers(boolean includeThisBroker) {
		try {
			List<Session> staleSessions = getStaleSessions(System.currentTimeMillis() - session.getTimeout() * 3);

			for (Session staleSession : staleSessions) {

				if (!includeThisBroker && session.getUuid().equals(staleSession.getUuid())) {
					logger.warn("This broker is in the list of stale brokers!");
				} else {
					logger.info(String.format("Found stale session %d, invalidating the session", staleSession.getSessionId()));

					zooKeeper.removeEphemeralSessionNodes(staleSession.getSessionId());

					// Delete broker session info only after session ephemeral nodes are removed.
					deleteSession(staleSession.getUuid());
				}
			}
		} catch (Throwable e) {
			logger.warn("Failed to invalidate stale brokers", e);
		}
	}

	public synchronized void start(int brokerId, long sessionId) {
		checkState(State.CREATED);
		this.brokerId = brokerId;
		session.setSessionId(sessionId);

		registerSession(String.valueOf(brokerId), session);

		// Stale brokers should be invalidated during startup. This is important in case one of the stale brokers is current broker with previous session.
		invalidateStaleBrokers(true);
		brokerInfoUpdater.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				updateBrokerInfo();
			}
		}, session.getTimeout() / 2, session.getTimeout() / 2, TimeUnit.MILLISECONDS);

		brokersChecker.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				invalidateStaleBrokers(false);
			}
		}, session.getTimeout(), session.getTimeout(), TimeUnit.MILLISECONDS);

		setState(State.STARTED);
	}

	private void updateBrokerInfo() {
		try {
			lastSeen = System.currentTimeMillis();
			session.setLastTimeSeen(lastSeen);
			updateSession(session);
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
				deleteSession(session.getUuid());
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
