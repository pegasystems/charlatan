package com.pega.charlatan.node.bean;


import com.pega.charlatan.io.Deserializable;
import com.pega.charlatan.io.ZookeeperReader;

import java.util.Objects;

/**
 * Access control list of the node.
 */
public class ACL implements Deserializable {
	private int permissions;
	private Id id;

	public ACL() {
	}

	public ACL(
			int permissions,
			Id id) {
		this.permissions = permissions;
		this.id = id;
	}

	public int getPermissions() {
		return permissions;
	}
	public Id getId() {
		return id;
	}

	@Override
	public void deserialize(ZookeeperReader reader) throws java.io.IOException {
		permissions =reader.readInt();
		id= new Id();
		id.deserialize(reader);
	}

	public boolean equals(Object obj) {
		if(obj instanceof ACL) {
			if(obj == this) {
				return true;
			}
			ACL other = (ACL) obj;
			return Objects.equals(permissions,other.permissions) && Objects.equals(id, other.id);
		}
		return false;
	}

	public int hashCode() {
		return Objects.hash(permissions, id);
	}
}
