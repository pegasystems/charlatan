package org.apache.zookeeper.dao.bean;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by natalia on 7/12/17.
 */
public class NodeTest {


	@Test
	public void getPathRoot() throws Exception {
		Node rootNode = new Node("/");
		assertEquals("/", rootNode.getPath());
	}

	@Test
	public void getPathEndsWithSlash() throws Exception {
		Node node = new Node( "/root/opt/java/");
		assertEquals("/root/opt/java", node.getPath());
	}

	@Test
	public void getPath() throws Exception {
		Node node = new Node( "/root/opt/java");
		assertEquals("/root/opt/java", node.getPath());
	}

	@Test
	public void getParentOfRoot() throws Exception {
		Node node = new Node( "/");
		Node parent = node.getParent();
		assertNull( parent );
	}
	@Test
	public void getParent() throws Exception {
		Node node = new Node( "/root/opt/java");
		Node parent = node.getParent();
		assertEquals( "/root/opt", parent.getPath() );
	}

	@Test
	public void getParent2() throws Exception {
		Node node = new Node( "/root");
		Node parent = node.getParent();
		assertEquals( "/", parent.getPath() );
	}

	@Test
	public void isRoot() throws Exception {
		Node node = new Node( "/root/opt/java");
		assertFalse( node.isRoot() );
	}

}