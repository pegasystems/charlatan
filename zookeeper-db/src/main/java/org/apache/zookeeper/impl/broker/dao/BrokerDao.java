package org.apache.zookeeper.impl.broker.dao;

import org.apache.zookeeper.impl.broker.bean.BrokerInfo;

import java.util.List;

/**
 * Created by natalia on 7/24/17.
 */
public interface BrokerDao {

	List<BrokerInfo> getBrokersInfo(long maxLastSeen);

	boolean delete(BrokerInfo brokerInfo);

	void update(BrokerInfo brokerInfo);
}
