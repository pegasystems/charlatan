package org.apache.zookeeper.impl.client;

import org.apache.zookeeper.impl.common.Service;
import org.apache.zookeeper.impl.node.bean.NodeUpdate;
import org.apache.zookeeper.impl.node.dao.DataAccessException;
import org.apache.zookeeper.impl.node.dao.RecordNotFoundException;
import org.apache.zookeeper.impl.watches.dao.NodeUpdateDao;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

@Service
public class NodeUpdateDaoRestClient extends NettyClient implements NodeUpdateDao {
	@Override
	public void insertUpdate(NodeUpdate update) {
		try {
			sendMessage(HttpMethod.POST, "/updates", update, null);
		} catch (IOException | RecordNotFoundException e) {
			throw new DataAccessException("Failed to create node update", e);
		}
	}


	@Override
	public List<NodeUpdate> getNodeUpdates(int ownerBroker, long fromTimestamp) {
		try {
			return Arrays.asList(sendMessage(HttpMethod.GET, "/updates?broker=" + ownerBroker + "&exclude_own=true&start_timestamp=" + fromTimestamp, NodeUpdate[].class));
		} catch (IOException | RecordNotFoundException e) {
			throw new DataAccessException("Failed to get node updates", e);
		}
	}

//	@Override
//	public void clearProcessedUpdates(int ownerBroker, int toId) {
//		try {
//			sendMessage(HttpMethod.DELETE, "/updates?broker=" + ownerBroker + "&end_id=" + toId);
//		} catch (IOException | RecordNotFoundException e) {
//			throw new DataAccessException("Failed to clear updates", e);
//		}
//	}

	@Override
	public void clearOldUpdates(long toMs) {
		try {
			sendMessage(HttpMethod.DELETE, "/updates?to_ms=" + toMs);
		} catch (IOException | RecordNotFoundException e) {
			throw new DataAccessException("Failed to clear old updates", e);
		}
	}

//	@Override
//	public int getLastUpdateId() {
//		try {
//			return sendMessage(HttpMethod.GET, "/updates/last/id", Integer.class);
//		} catch (IOException | RecordNotFoundException e) {
//			throw new DataAccessException("Failed to retrieve last update id", e);
//		}
//	}
}
