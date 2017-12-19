/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pega.charlatan.watches.bean;

import com.pega.charlatan.watches.bean.Watcher.Event.Type;
import com.pega.charlatan.watches.bean.Watcher.Event.State;

/**
 *  A WatchedEvent represents a change on the ZooKeeper that a Watcher
 *  is able to respond to.  The WatchedEvent includes exactly what happened,
 *  the current state of the ZooKeeper, and the path of the znode that
 *  was involved in the event.
 */
public class WatchedEvent {
    final private State state;
    final private Type type;
    private String path;
    
    /**
     * Create a WatchedEvent with specified type, state and path
     */
    public WatchedEvent(Type type, State state, String path) {
        this.state = state;
        this.type = type;
        this.path = path;
    }

	public WatchedEvent(int eventType, int keeperState, String path) {
		this.state = State.fromCode(keeperState);
		this.type = Type.fromCode(eventType);
		this.path = path;
	}

	public State getState() {
        return state;
    }
    
    public Type getType() {
        return type;
    }
    
    public String getPath() {
        return path;
    }

    @Override
    public String toString() {
        return "WatchedEvent state:" + state
            + " type:" + type + " path:" + path;
    }
}
