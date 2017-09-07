package com.pega.zooikeeper.io;

import java.io.IOException;

public interface Serializable {
	void serialize(ZookeeperWriter writer) throws IOException;
}
