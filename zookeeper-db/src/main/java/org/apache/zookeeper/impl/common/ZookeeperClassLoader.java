package org.apache.zookeeper.impl.common;

import org.apache.zookeeper.impl.node.dao.NodeDao;
import org.apache.zookeeper.impl.node.service.ZKDatabase;
import org.apache.zookeeper.impl.watches.dao.NodeUpdateDao;
import org.apache.zookeeper.impl.watches.service.RemoteNodeUpdates;
import org.apache.zookeeper.impl.watches.service.RemoteNodeUpdatesDbImpl;
import org.reflections.ReflectionUtils;
import org.reflections.Reflections;
import org.reflections.scanners.Scanner;
import org.reflections.scanners.SubTypesScanner;
import org.reflections.util.ClasspathHelper;
import org.reflections.util.ConfigurationBuilder;
import org.reflections.util.FilterBuilder;

import java.lang.annotation.Annotation;
import java.util.Set;

/**
 * Created by natalia on 7/19/17.
 */
public class ZookeeperClassLoader {
	private static Reflections reflections= //new Reflections(ClasspathHelper.forPackage("org.apache.zookeeper", ClasspathHelper.contextClassLoader()));
	new Reflections(new ConfigurationBuilder()
				.addUrls(ClasspathHelper.forPackage("org.apache.zookeeper"))
	            .setExpandSuperTypes(false));

	private static NodeDao nodeDaoImpl = retrieveNodeDabImpl();
	private static RemoteNodeUpdates remoteNodeUpdates = loadRemoteNodeUpdatesImpl();

	private static NodeDao retrieveNodeDabImpl() {

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
			return daoImplClass.newInstance();
		} catch (Exception e) {
			throw new ZookeeperRuntimeException("Failed to instantiate NodeDao", e);
		}
	}

	private static RemoteNodeUpdates loadRemoteNodeUpdatesImpl() {
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

			return RemoteNodeUpdatesDbImpl.class.getConstructor(NodeUpdateDao.class).newInstance(nodeUpdateDao);
		} catch (Exception e) {
			throw new ZookeeperRuntimeException("Failed to instantiate NodeUpdateDao", e);
		}
	}

	public static NodeDao getNodeDao() {
		return nodeDaoImpl;
	}

	public static RemoteNodeUpdates getRemoteNodeUpdates() {
		return remoteNodeUpdates;
	}
}
