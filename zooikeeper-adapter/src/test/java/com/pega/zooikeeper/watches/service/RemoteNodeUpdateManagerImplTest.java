package com.pega.zooikeeper.watches.service;

import com.google.common.collect.Lists;
import com.pega.zooikeeper.node.bean.NodeUpdate;
import com.pega.zooikeeper.watches.dao.NodeUpdateDao;
import org.apache.zookeeper.Watcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(Parameterized.class)
public class RemoteNodeUpdateManagerImplTest {

	private static long timestamp1 = 1504082581000l; //August 30, 2017 8:43:01 AM
	private static long timestamp2 = 1504083600000l; //August 30, 2017 9:00:00 AM

	private static NodeUpdate update1WithTimestamp1 = new NodeUpdate(Watcher.Event.EventType.NodeCreated, "/test/child1", timestamp1, 2);
	private static NodeUpdate update2WithTimestamp2 = new NodeUpdate(Watcher.Event.EventType.NodeCreated, "/test/child2", timestamp2, 3);
	private static NodeUpdate update3WithTimestamp2 = new NodeUpdate(Watcher.Event.EventType.NodeCreated, "/test/child3", timestamp2, 2);

	private static Set<NodeUpdate> updatesWithTimestamp1_1 = new HashSet<>();
	static {
		updatesWithTimestamp1_1.add(update1WithTimestamp1);
	}

	private static Set<NodeUpdate> updatesWithTimestamp2_1 = new HashSet<>();
	static {
		updatesWithTimestamp2_1.add(update2WithTimestamp2);
	}

	private static Set<NodeUpdate> updatesWithTimestamp2_2 = new HashSet<>();
	static {
		updatesWithTimestamp2_2.add(update2WithTimestamp2);
		updatesWithTimestamp2_2.add(update3WithTimestamp2);
	}

	private static int BROKER_ID = 1;


	private long latestTimestampBeforePull;
	private Set<NodeUpdate> latestUpdatesBeforePull;

	private List<NodeUpdate> updatesFromLatestTimestamp;

	private long latestTimestampAfterPull;
	private Set<NodeUpdate> latestUpdateAfterPull;

	public RemoteNodeUpdateManagerImplTest(long latestTimestampBeforePull, Set<NodeUpdate> latestUpdatesBeforePull, List<NodeUpdate> updatesFromLatestTimestamp,long latestTimestampAfterPull, Set<NodeUpdate> latestUpdateAfterPull, String testName ){
		this.latestTimestampBeforePull = latestTimestampBeforePull;
		this.latestUpdatesBeforePull = latestUpdatesBeforePull;
		this.updatesFromLatestTimestamp = updatesFromLatestTimestamp;
		this.latestTimestampAfterPull = latestTimestampAfterPull;
		this.latestUpdateAfterPull = latestUpdateAfterPull;
	}

	@Parameters(name = "{5}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
				{timestamp1, new HashSet<NodeUpdate>(), Lists.newArrayList(),
						timestamp1,	new HashSet<NodeUpdate>(), "Before: no updates. Pull: no updates. After: no updates"
				},
				{timestamp1, new HashSet<NodeUpdate>(), Lists.newArrayList(update1WithTimestamp1, update2WithTimestamp2),
						timestamp2, updatesWithTimestamp2_1, "Before: no updates. Pull: 2 updates/different timestamp. After: one update with biggest timestamp"
				},
				{timestamp1, new HashSet<NodeUpdate>(), Lists.newArrayList(update1WithTimestamp1, update2WithTimestamp2, update3WithTimestamp2),
						timestamp2, updatesWithTimestamp2_2, "Before: no updates. Pull: 3 updates/different timestamp. After: two update with biggest timestamp"
				},
				{timestamp2, updatesWithTimestamp2_1, Lists.newArrayList(update2WithTimestamp2, update3WithTimestamp2),
						timestamp2 + 1, new HashSet<NodeUpdate>(), "Before: 1 update. Pull: 2 updates/same timestamp. After: no updates, timestamp increased"
				},
				{timestamp2, updatesWithTimestamp2_2, Lists.newArrayList(update2WithTimestamp2, update3WithTimestamp2),
						timestamp2 + 1, new HashSet<NodeUpdate>(), "Before: 2 updates. Pull: 2 same updates. After: no updates, timestamp increased"
				},
				{timestamp2, updatesWithTimestamp2_2, Lists.newArrayList(),
						timestamp2, updatesWithTimestamp2_2, "Before: has updates. Pull: no updates. After: same updates"}
		});
	}

	@Test
	public void pullUpdatesTest() throws Exception {
		NodeUpdateDao nodeUpdateDao = mock(NodeUpdateDao.class);
		when(nodeUpdateDao.getNodeUpdates(BROKER_ID, latestTimestampBeforePull)).thenReturn(updatesFromLatestTimestamp);

		RemoteNodeUpdateManagerImpl remoteNodeUpdateManager = new RemoteNodeUpdateManagerImpl(nodeUpdateDao, latestTimestampBeforePull, BROKER_ID);
		remoteNodeUpdateManager.lastCheckedTimestamp = latestTimestampBeforePull;
		remoteNodeUpdateManager.lastCheckedNodeUpdates = latestUpdatesBeforePull;

		// First pull, highest timestamp is timestamp2
		remoteNodeUpdateManager.pullUpdates();
		assertEquals(latestTimestampAfterPull, remoteNodeUpdateManager.lastCheckedTimestamp);
		assertEquals(latestUpdateAfterPull, remoteNodeUpdateManager.lastCheckedNodeUpdates);
	}

//	@Test
//	public void pullUpdates1() throws Exception {
//
//		long timestamp1 = 1504082581000l; //August 30, 2017 8:43:01 AM
//		long timestamp2 = 1504083600000l; //August 30, 2017 9:00:00 AM
//
//		NodeUpdate update1WithTimestamp1 = new NodeUpdate(Watcher.Event.EventType.NodeCreated, "/test/child", timestamp1, 2);
//		NodeUpdate update2WithTimestamp2 = new NodeUpdate(Watcher.Event.EventType.NodeCreated, "/test/child2", timestamp2, 3);
//		NodeUpdate update3WithTimestamp2 = new NodeUpdate(Watcher.Event.EventType.NodeCreated, "/test/child3", timestamp2, 2);
//
//		List<NodeUpdate> nodeUpdatesFromTimestamp1 = Lists.newArrayList(update1WithTimestamp1, update2WithTimestamp2, update3WithTimestamp2);
//		List<NodeUpdate> nodeUpdatesFromTimestamp2 = Lists.newArrayList(update2WithTimestamp2, update3WithTimestamp2);
//
//		Set<NodeUpdate> nodeUpdatesWithBiggerTimestamp = new HashSet<>(nodeUpdatesFromTimestamp2);
//
//		NodeUpdateDao nodeUpdateDao = mock(NodeUpdateDao.class);
//		when(nodeUpdateDao.getNodeUpdates(BROKER_ID, timestamp1)).thenReturn(nodeUpdatesFromTimestamp1);
//		when(nodeUpdateDao.getNodeUpdates(BROKER_ID, timestamp2)).thenReturn(nodeUpdatesFromTimestamp2);
//
//		RemoteNodeUpdateManagerImpl remoteNodeUpdateManager = new RemoteNodeUpdateManagerImpl(nodeUpdateDao, timestamp1, BROKER_ID);
//
//		// First pull, highest timestamp is timestamp2
//		remoteNodeUpdateManager.pullUpdates();
//		assertEquals(timestamp2, remoteNodeUpdateManager.lastCheckedTimestamp);
//		assertEquals(nodeUpdatesWithBiggerTimestamp, remoteNodeUpdateManager.lastCheckedNodeUpdates);
//
//		// Second pull, highest timestamp didn't change, nothing is processed
//		remoteNodeUpdateManager.pullUpdates();
//		assertEquals(timestamp2 + 1, remoteNodeUpdateManager.lastCheckedTimestamp);
//		assertTrue(remoteNodeUpdateManager.lastCheckedNodeUpdates.isEmpty());
//	}

}