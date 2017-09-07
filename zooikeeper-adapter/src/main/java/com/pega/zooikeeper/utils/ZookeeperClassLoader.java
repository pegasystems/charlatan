package com.pega.zooikeeper.utils;

import com.pega.zooikeeper.broker.dao.BrokerDao;
import com.pega.zooikeeper.node.dao.NodeDao;

import com.pega.zooikeeper.watches.dao.NodeUpdateDao;
import com.pega.zooikeeper.watches.service.WatchCache;
import com.pega.zooikeeper.watches.service.WatchCacheImpl;
import com.pega.zooikeeper.watches.service.WatchService;
import com.pega.zooikeeper.watches.service.WatchServiceImpl;
import org.reflections.ReflectionUtils;
import org.reflections.Reflections;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;

import java.lang.annotation.Annotation;
import java.util.Set;

/**
 * Loads dao implementations from classpath on runtime
 */
public class ZookeeperClassLoader {
	private static Reflections reflections= //new Reflections(ClasspathHelper.forPackage("org.apache.zookeeper", ClasspathHelper.contextClassLoader()));
	new Reflections(new ConfigurationBuilder()
				.addUrls(ClasspathHelper.forPackage("org.apache.zookeeper")));

//	            .setExpandSuperTypes(false));

	private static NodeDao nodeDaoImpl;
	private static BrokerDao brokerDaoImpl;
	private static WatchService watchService;

	static {
		loadNodeDaoImpl();
		loadBrokerDaoImpl();
		loadRemoteNodeUpdatesImpl();
	}

	private static void loadNodeDaoImpl() {

		Set<Class<? extends NodeDao>> daoImpls = reflections.getSubTypesOf(NodeDao.class);
		if (daoImpls.size() == 0) {
			throw new RuntimeException("No NodeDao implementation found");
		}

		Class<? extends NodeDao> daoImplClass = null;

		for (Class<? extends NodeDao> daoImpl : daoImpls) {
			Set<Annotation> annotations = ReflectionUtils.getAnnotations(daoImpl);
			for (Annotation annotation : annotations) {
				if (annotation.annotationType().equals(Service.class)) {
					if (daoImplClass == null) {
						daoImplClass = daoImpl;
					} else {
						throw new RuntimeException("Multiple NodeDao implementations found");
					}
				}
			}
		}

		if (daoImplClass == null) {
			throw new RuntimeException(
					"No NodeDao implementation found");
		}

		try {
			nodeDaoImpl = daoImplClass.newInstance();
		} catch (Exception e) {
			throw new ZookeeperRuntimeException("Failed to instantiate NodeDao", e);
		}
	}

	private static void loadRemoteNodeUpdatesImpl() {
		Set<Class<? extends NodeUpdateDao>> NodeUpdateDaoImpls = reflections.getSubTypesOf(NodeUpdateDao.class);

		if (NodeUpdateDaoImpls.size() == 0) {
			throw new RuntimeException("No NodeUpdateDao implementation found");
		}

		if (NodeUpdateDaoImpls.size() > 1) {
			throw new RuntimeException("Multiple NodeUpdateDao implementations found");
		}

		Class<? extends NodeUpdateDao> nodeUpdateDaoImpl = NodeUpdateDaoImpls.iterator().next();

		try {
			NodeUpdateDao nodeUpdateDao = nodeUpdateDaoImpl.newInstance();

			watchService = WatchServiceImpl.class.getConstructor(WatchCache.class, NodeUpdateDao.class).newInstance(new WatchCacheImpl(),nodeUpdateDao);
		} catch (Exception e) {
			throw new ZookeeperRuntimeException("Failed to instantiate NodeUpdateDao", e);
		}
	}

	private static void loadBrokerDaoImpl() {
		Set<Class<? extends BrokerDao>> brokerDaoImpls = reflections.getSubTypesOf(BrokerDao.class);

		if (brokerDaoImpls.size() == 0) {
			throw new RuntimeException("No BrokerDao implementation found");
		}

		if (brokerDaoImpls.size() > 1) {
			throw new RuntimeException("Multiple BrokerDao implementations found");
		}

		Class<? extends BrokerDao> brokerDao = brokerDaoImpls.iterator().next();

		try {
			brokerDaoImpl = brokerDao.newInstance();

		} catch (Exception e) {
			throw new ZookeeperRuntimeException("Failed to instantiate BrokerDao", e);
		}
	}

	public static NodeDao getNodeDao() {
		return nodeDaoImpl;
	}

	public static WatchService getWatchService() {
		return watchService;
	}

	public static BrokerDao getBrokerDaoImpl() {
		return brokerDaoImpl;
	}
}
