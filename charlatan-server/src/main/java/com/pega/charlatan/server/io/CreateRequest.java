package com.pega.charlatan.server.io;

import com.pega.charlatan.io.Deserializable;
import com.pega.charlatan.io.ZookeeperReader;
import com.pega.charlatan.node.bean.ACL;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class CreateRequest implements Deserializable {

	private String path;
	private byte[] data;
	private List<ACL> acl;
	private int flags;

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public byte[] getData() {
		return data;
	}

	public void setData(byte[] data) {
		this.data = data;
	}

	public List<ACL> getAcl() {
		return acl;
	}

	public void setAcl(List<ACL> acl) {
		this.acl = acl;
	}

	public int getFlags() {
		return flags;
	}

	public void setFlags(int flags) {
		this.flags = flags;
	}

	@Override
	public void deserialize(ZookeeperReader reader) throws IOException {
		path = reader.readString();
		data = reader.readBuffer();
		int aclLength = reader.readInt();

		if(aclLength > 0 ) {
			acl = new ArrayList<>(aclLength);
			for (int i = 0; i < aclLength; i++) {
				ACL a = new ACL();
				a.deserialize(reader);
				acl.add(a);
			}
		}

		flags = reader.readInt();
	}

	@Override
	public String toString(){
		return String.format("Create: %s", path);
	}
}
