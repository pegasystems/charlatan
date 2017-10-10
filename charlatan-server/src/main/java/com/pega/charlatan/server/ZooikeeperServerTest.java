package com.pega.charlatan.server;

import com.pega.charlatan.server.session.service.SessionServiceImpl;
import com.pega.charlatan.sqlite.SessionDaoSqlite;
import com.pega.charlatan.utils.NamedThreadFactory;
import com.pega.charlatan.watches.service.WatchServiceImpl;

public class ZooikeeperServerTest {
	public static void main(String[] args) {

		org.apache.log4j.BasicConfigurator.configure();

		int port = 2181;
		if(args != null && args.length == 1){
			port  = Integer.parseInt(args[0]);
		}

		ZooikeeperNettyServer server = new ZooikeeperServerBuilder()
				.setHost("localhost")
				.setPort(port)
				.setId("server"+port)
				.setWorkerCount(5)
				.setSecure(true)
				.setNodeDao(new com.pega.charlatan.sqlite.NodeDaoSqlite())
				.setWatchService(new WatchServiceImpl(new com.pega.charlatan.sqlite.NodeUpdateDaoSqlite(), new NamedThreadFactory("test-zooikeeper")))
				.setSessionService(new SessionServiceImpl(new SessionDaoSqlite()))
				.setThreadFactory(new NamedThreadFactory("zooikeeper"))
				.build();

		String host = "localhost";
		int port = 2181;

		ZooikeeperNettyServer server = new ZooikeeperServerBuilder()
				// Zooikeeper host
				.setHost(host)
				// Zooikeeper port
				.setPort(port)
				// Zooikeeper id must be unique per Zooikeeper cluster
				.setId("server"+port)
				.setWorkerCount(5)
				// Node DAO implementation
				.setNodeDao(new NodeDaoSqlite())
				// Watch service implementation
				.setWatchService(new WatchServiceImpl(NodeUpdateDaoSqlite(), new NamedThreadFactory("zooikeeper-watch-service")))
				// Session service implementation
				.setSessionService(new SessionServiceImpl(new SessionDaoSqlite()))
				.setThreadFactory(new NamedThreadFactory("zooikeeper-service"))
				.build();

		server.start();
	}
}
