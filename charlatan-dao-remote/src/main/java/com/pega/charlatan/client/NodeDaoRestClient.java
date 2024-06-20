package com.pega.charlatan.client;

import com.pega.charlatan.node.dao.DataAccessException;
import com.pega.charlatan.node.dao.NodeDao;
import com.pega.charlatan.node.dao.RecordNotFoundException;
import com.pega.charlatan.utils.Service;
import com.pega.charlatan.node.bean.Node;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Arrays;
import java.util.List;

@Service
public class NodeDaoRestClient extends NettyClient implements NodeDao {
	@Override
	public boolean create(long session, Node node) throws RecordNotFoundException {
		try {
			return sendMessage(HttpMethod.POST, "/nodes?session=" + session, node, Boolean.class);
		} catch (IOException e) {
			logger.error("Failed to create requested node", e);
			throw new DataAccessException("Failed to create requested node", e);
		}
	}

	@Override
	public boolean delete(Node node) {
		try {
			return sendMessage(HttpMethod.DELETE, "/nodes/" + encodePath(node.getPath()) + "?version=" + node.getState().getVersion(), Boolean.class);
		} catch (IOException | RecordNotFoundException e) {
			throw new DataAccessException("Failed to delete requested node", e);
		}
	}

	@Override
	public Node get(String path) throws RecordNotFoundException {
		try {
			return sendMessage(HttpMethod.GET, "/nodes/" + encodePath(path), Node.class);
		} catch (IOException e) {
			throw new DataAccessException("Failed to delete requested node", e);
		}
	}

	@Override
	public void update(String path, byte[] data, int newVersion, long modificationTime) {
		try {
			// TDOD: check if it is possible to create body instead of parameters(is PATCH supported?)
			sendMessage(HttpMethod.PUT, "/nodes/" + encodePath(path) + "?version=" + newVersion + "&mtime=" + modificationTime, data, null);
		} catch (IOException | RecordNotFoundException e) {
			throw new DataAccessException("Failed to update requested node", e);
		}
	}

	@Override
	public void updateCVersion(String path, int cversion) {
		try {
			sendMessage(HttpMethod.PUT, "/nodes/" + encodePath(path) + "?cversion=" + cversion);
		} catch (IOException | RecordNotFoundException e) {
			throw new DataAccessException("Failed to update node cversion", e);
		}
	}

	@Override
	public List<String> getEphemeralPaths(long session) {
		try {
			return Arrays.asList(sendMessage(HttpMethod.GET, "/nodes?session=" + session + "&mode=ephimeral", String[].class));
		} catch (IOException | RecordNotFoundException e) {
			throw new DataAccessException("Failed to get ephemeral nodes of the session", e);
		}
	}

	private String encodePath(String path) {
		try {
			return URLEncoder.encode(path, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			return path;
		}
	}
}
