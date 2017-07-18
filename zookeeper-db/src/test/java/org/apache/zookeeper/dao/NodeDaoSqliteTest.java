package org.apache.zookeeper.dao;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.bean.Node;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class NodeDaoSqliteTest {

	private static NodeDaoSqlite dao;
	private static String session;

	private Node TEST_NODE = new Node("/test/path", null, CreateMode.PERSISTENT);

	@BeforeClass
	public static void setupDb() {
		dao = new NodeDaoSqlite(System.currentTimeMillis() + ".db");
		dao.setup();
		session = "test";
	}

	@Test(expected = RecordNotFoundException.class)
	public void test1_CreateWithNoParent() throws RecordNotFoundException {
		dao.create(TEST_NODE);
	}

	@Test
	public void test2_Create() throws RecordNotFoundException {
		dao.create(new Node("/test", null));
		dao.create(TEST_NODE);

		assertTrue(dao.exists(TEST_NODE));
	}

	@Test
	public void test3_CreateExisted() throws RecordNotFoundException {
		assertFalse(dao.create(TEST_NODE));
	}

	@Test
	public void test4_Get() throws RecordNotFoundException {
		Node node = dao.get(TEST_NODE.getPath());
		assertEquals(TEST_NODE.getPath(), node.getPath());
	}

	@Test
	public void test5_GetChildren() throws RecordNotFoundException {
		dao.create( new Node(TEST_NODE.getPath() + "/child") );
		List<String> childrenNodes = dao.getChildren(TEST_NODE);
		assertEquals(Arrays.asList("child"), childrenNodes );
	}

	@Test
	public void test6_Delete() {

		dao.delete(TEST_NODE);
		assertFalse(dao.exists(TEST_NODE));
	}

	public void testUpdate() {
	}

}
