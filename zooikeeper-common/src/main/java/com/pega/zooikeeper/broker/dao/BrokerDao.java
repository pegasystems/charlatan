package com.pega.zooikeeper.broker.dao;

import com.pega.zooikeeper.broker.bean.BrokerInfo;

import java.util.List;

/**
 * Created by natalia on 7/24/17.
 */
public interface BrokerDao {

	List<BrokerInfo> getBrokersInfo(long maxLastSeen);

	boolean delete(BrokerInfo brokerInfo);

	void update(BrokerInfo brokerInfo);
}
