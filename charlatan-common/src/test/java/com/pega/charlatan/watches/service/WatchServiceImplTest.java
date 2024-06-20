package com.pega.charlatan.watches.service;

import com.pega.charlatan.node.bean.NodeUpdate;
import com.pega.charlatan.utils.NamedThreadFactory;
import com.pega.charlatan.watches.dao.NodeUpdateDao;
import com.pega.charlatan.watches.bean.Watcher;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.mockito.Mockito;

import java.util.*;

import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class WatchServiceImplTest {

	private static long timestamp1 = 1504082581000l; //August 30, 2017 8:43:01 AM
	private static long timestamp2 = 1504083600000l; //August 30, 2017 9:00:00 AM

	private static NodeUpdate update1WithTimestamp1 = new NodeUpdate(Watcher.Event.Type.NodeCreated, "/test/child1", timestamp1, 2);
	private static NodeUpdate update2WithTimestamp2 = new NodeUpdate(Watcher.Event.Type.NodeCreated, "/test/child2", timestamp2, 3);
	private static NodeUpdate update3WithTimestamp2 = new NodeUpdate(Watcher.Event.Type.NodeCreated, "/test/child3", timestamp2, 2);

	private static Set<NodeUpdate> updatesWithTimestamp1_1 = new HashSet<>();
	private static Set<NodeUpdate> updatesWithTimestamp2_1 = new HashSet<>();
	private static Set<NodeUpdate> updatesWithTimestamp2_2 = new HashSet<>();
	private static int BROKER_ID = 1;

	static {
		updatesWithTimestamp1_1.add(update1WithTimestamp1);
	}

	static {
		updatesWithTimestamp2_1.add(update2WithTimestamp2);
	}

	static {
		updatesWithTimestamp2_2.add(update2WithTimestamp2);
		updatesWithTimestamp2_2.add(update3WithTimestamp2);
	}

	private long latestTimestampBeforePull;
	private Set<NodeUpdate> latestUpdatesBeforePull;

	private List<NodeUpdate> updatesFromLatestTimestamp;

	private long latestTimestampAfterPull;
	private Set<NodeUpdate> latestUpdateAfterPull;

	public WatchServiceImplTest(long latestTimestampBeforePull, Set<NodeUpdate> latestUpdatesBeforePull, NodeUpdate[] updatesFromLatestTimestamp, long latestTimestampAfterPull, Set<NodeUpdate> latestUpdateAfterPull, String testName) {
		this.latestTimestampBeforePull = latestTimestampBeforePull;
		this.latestUpdatesBeforePull = latestUpdatesBeforePull;
		this.updatesFromLatestTimestamp = Arrays.asList(updatesFromLatestTimestamp);
		this.latestTimestampAfterPull = latestTimestampAfterPull;
		this.latestUpdateAfterPull = latestUpdateAfterPull;
	}

	@Parameters(name = "{5}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][]{
				{timestamp1, new HashSet<NodeUpdate>(), new NodeUpdate[]{},
						timestamp1, new HashSet<NodeUpdate>(), "Before: no updates. Pull: no updates. After: no updates"
				},
				{timestamp1, new HashSet<NodeUpdate>(), new NodeUpdate[]{update1WithTimestamp1, update2WithTimestamp2},
						timestamp2, updatesWithTimestamp2_1, "Before: no updates. Pull: 2 updates/different timestamp. After: one update with biggest timestamp"
				},
				{timestamp1, new HashSet<NodeUpdate>(), new NodeUpdate[]{update1WithTimestamp1, update2WithTimestamp2, update3WithTimestamp2},
						timestamp2, updatesWithTimestamp2_2, "Before: no updates. Pull: 3 updates/different timestamp. After: two update with biggest timestamp"
				},
				{timestamp2, updatesWithTimestamp2_1, new NodeUpdate[]{update2WithTimestamp2, update3WithTimestamp2},
						timestamp2 + 1, new HashSet<NodeUpdate>(), "Before: 1 update. Pull: 2 updates/same timestamp. After: no updates, timestamp increased"
				},
				{timestamp2, updatesWithTimestamp2_2, new NodeUpdate[]{update2WithTimestamp2, update3WithTimestamp2},
						timestamp2 + 1, new HashSet<NodeUpdate>(), "Before: 2 updates. Pull: 2 same updates. After: no updates, timestamp increased"
				},
				{timestamp2, updatesWithTimestamp2_2, new NodeUpdate[]{},
						timestamp2, updatesWithTimestamp2_2, "Before: has updates. Pull: no updates. After: same updates"}
		});
	}

	@Test
	public void pullUpdatesTest() throws Exception {
		NodeUpdateDao nodeUpdateDao = Mockito.mock(NodeUpdateDao.class);
		Mockito.when(nodeUpdateDao.getNodeUpdates(BROKER_ID, latestTimestampBeforePull)).thenReturn(updatesFromLatestTimestamp);

		WatchCache watchCache = Mockito.mock(WatchCache.class);

		WatchServiceImpl remoteNodeUpdateManager = new WatchServiceImpl(watchCache, nodeUpdateDao, latestTimestampBeforePull, BROKER_ID, new NamedThreadFactory("watcher"));
		remoteNodeUpdateManager.lastCheckedTimestamp = latestTimestampBeforePull;
		remoteNodeUpdateManager.lastCheckedNodeUpdates = latestUpdatesBeforePull;

		// First pull, highest timestamp is timestamp2
		remoteNodeUpdateManager.pullUpdates();

		assertEquals(latestTimestampAfterPull, remoteNodeUpdateManager.lastCheckedTimestamp);
		assertEquals(latestUpdateAfterPull, remoteNodeUpdateManager.lastCheckedNodeUpdates);
	}
}