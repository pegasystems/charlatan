package com.pega.charlatan.io;

import java.io.IOException;

public interface Deserializable {
	void deserialize(ZookeeperReader reader) throws IOException;
}
