package com.pega.charlatan.watches.bean;

public interface Watcher {
	void process(WatchedEvent event);

	/**
	 * Enumeration of types of watchers
	 */
	enum Type {
		/*
		 * deletes the node of the given path or creates/delete a child under the node.
		 */
		Children,
		/*
		 * sets data on the node, or deletes the node.
		 */
		Data,
		Exist;
	}


	/**
	 * This interface defines the possible states an Event may represent
	 */
	interface Event {
		/**
		 * Enumeration of states the ZooKeeper may be at the event
		 */
		enum State {
			/**
			 * The client is in the disconnected state - it is not connected to any server in the ensemble.
			 */
			Disconnected(0),

			/**
			 * The client is in the connected state - it is connected to a server in the ensemble (one of the servers
			 * specified in the host connection parameter during ZooKeeper client creation).
			 */
			SyncConnected(3),

			/**
			 * Auth failed state
			 */
			AuthFailed(4),

			/**
			 * The client is connected to a read-only server, that is the server which is not currently connected to the
			 * majority. The only operations allowed after receiving this state is read operations. This state is
			 * generated for read-only clients only since read/write clients aren't allowed to connect to r/o servers.
			 */
			ConnectedReadOnly(5),

			/**
			 * SaslAuthenticated: used to notify clients that they are SASL-authenticated, so that they can perform
			 * Zookeeper actions with their SASL-authorized permissions.
			 */
			SaslAuthenticated(6),

			/**
			 * The serving cluster has expired this session. The ZooKeeper client connection (the session) is no longer
			 * valid. You must create a new client connection (instantiate a new ZooKeeper instance) if you with to
			 * access the ensemble.
			 */
			Expired(-112);

			private final int code;     // Integer representation of value
			// for sending over wire

			State(int code) {
				this.code = code;
			}

			public static State fromCode(int code) {
				switch (code) {
					case 0:
						return State.Disconnected;
					case 3:
						return State.SyncConnected;
					case 4:
						return State.AuthFailed;
					case 5:
						return State.ConnectedReadOnly;
					case 6:
						return State.SaslAuthenticated;
					case -112:
						return State.Expired;

					default:
						throw new RuntimeException("Invalid integer value for conversion to KeeperState");
				}
			}

			public int getCode() {
				return code;
			}
		}

		/**
		 * Enumeration of types of events that may occur on the ZooKeeper
		 */
		enum Type {
			None(-1),
			NodeCreated(1),
			NodeDeleted(2),
			NodeDataChanged(3),
			NodeChildrenChanged(4);

			private final int code;     // Integer representation of value
			// for sending over wire

			Type(int intValue) {
				this.code = intValue;
			}

			public static Type fromCode(int intValue) {
				switch (intValue) {
					case -1:
						return Type.None;
					case 1:
						return Type.NodeCreated;
					case 2:
						return Type.NodeDeleted;
					case 3:
						return Type.NodeDataChanged;
					case 4:
						return Type.NodeChildrenChanged;

					default:
						throw new RuntimeException("Invalid integer value for conversion to EventType");
				}
			}

			public int getCode() {
				return code;
			}
		}
	}
}
