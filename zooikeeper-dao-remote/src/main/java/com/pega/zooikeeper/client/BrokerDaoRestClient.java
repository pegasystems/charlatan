package com.pega.zooikeeper.client;

import com.pega.zooikeeper.broker.bean.BrokerInfo;
import com.pega.zooikeeper.broker.dao.BrokerDao;
import com.pega.zooikeeper.node.dao.DataAccessException;
import com.pega.zooikeeper.node.dao.RecordNotFoundException;
import com.pega.zooikeeper.utils.Service;

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
