package org.apache.zookeeper.impl.client;

import org.apache.zookeeper.impl.broker.bean.BrokerInfo;
import org.apache.zookeeper.impl.broker.dao.BrokerDao;
import org.apache.zookeeper.impl.common.Service;
import org.apache.zookeeper.impl.node.dao.DataAccessException;
import org.apache.zookeeper.impl.node.dao.RecordNotFoundException;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Service
public class BrokerDaoRestClient extends NettyClient implements BrokerDao {
	@Override
	public List<BrokerInfo> getBrokersInfo(long maxLastSeen) {
		try {
			return Arrays.asList(sendMessage(HttpMethod.GET, "/brokers?maxLastSeen=" + maxLastSeen, BrokerInfo[].class));
		} catch (IOException | RecordNotFoundException e) {
			logger.error("Failed to retrieve broker info", e);
			throw new DataAccessException("Failed to get expired brokers", e);
		}
	}

	@Override
	public boolean delete(BrokerInfo brokerInfo) {
		try {
			return sendMessage(HttpMethod.DELETE, "/brokers/" + brokerInfo.getBrokerId() + "?session=" + brokerInfo.getSession() + "&last_seen=" + brokerInfo.getLastTimeSeen(), Boolean.class);
		} catch (IOException | RecordNotFoundException e) {
			logger.error("Failed to delete broker info", e);
			throw new DataAccessException("Failed to delete broker info", e);
		}
	}

	@Override
	public void update(BrokerInfo brokerInfo) {
		try {
			sendMessage(HttpMethod.POST, "/brokers/" + brokerInfo.getBrokerId(), brokerInfo, null);
		} catch (IOException | RecordNotFoundException e) {
			logger.error("Failed to update broker info", e);
			throw new DataAccessException("Failed to update broker info", e);
		}
	}
}
