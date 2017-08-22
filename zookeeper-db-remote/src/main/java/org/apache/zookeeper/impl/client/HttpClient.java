//package org.apache.zookeeper.impl.client;
//
//import com.fasterxml.jackson.databind.DeserializationFeature;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import org.apache.http.HttpEntity;
//import org.apache.http.HttpHost;
//import org.apache.http.HttpResponse;
//import org.apache.http.HttpStatus;
//import org.apache.http.client.methods.*;
//import org.apache.http.entity.ByteArrayEntity;
//import org.apache.http.impl.client.CloseableHttpClient;
//import org.apache.http.impl.client.HttpClientBuilder;
//import org.apache.zookeeper.impl.node.dao.RecordNotFoundException;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.IOException;
//
///**
// * Uses apache http client
// */
//public class HttpClient {
//
//	protected static Logger logger = LoggerFactory.getLogger(HttpClient.class.getName());
//	private volatile CloseableHttpClient httpClient;
//	private String host;
//	private int port;
//	private ObjectMapper mapper;
//
//	public HttpClient() {
//		String host = System.getProperty("ZOOKEEPER_HOST");
//		String port = System.getProperty("ZOOKEEPER_PORT");
//
//		if (host == null && host.isEmpty()) {
//			throw new RuntimeException("Zookeeper server host is unknown");
//		}
//
//		if (port == null && port.isEmpty()) {
//			throw new RuntimeException("Zookeeper server port is unknown");
//		}
//
//		init(host, Integer.parseInt(port));
//	}
//
//	public HttpClient(String host, int port) {
//		init(host, port);
//	}
//
//	private CloseableHttpClient getHttpClient() {
////		if (httpClient == null) {
////			synchronized (this) {
////				if (httpClient == null) {
////					final RequestConfig params = RequestConfig.custom().setConnectTimeout(10*60*1000).setSocketTimeout(10*60*1000).build();
////					httpClient = HttpClientBuilder.create().setDefaultRequestConfig(params).build();
////				}
////			}
////		}
////		return httpClient;
//
//		return HttpClientBuilder.create().build();
//	}
//
//	private void init(String host, int port) {
//		this.host = host;
//		this.port = port;
//		this.mapper = new ObjectMapper();
//		this.mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
//	}
//
//	protected void sendMessage(HttpMethod httpMethod, String api) throws IOException, RecordNotFoundException {
//		sendMessage(httpMethod, api, null);
//	}
//
//	protected <T> T sendMessage(HttpMethod httpMethod, String api, Class<T> responseType) throws IOException, RecordNotFoundException {
//		return sendMessage(httpMethod, api, null, responseType);
//	}
//
//	protected <T> T sendMessage(HttpMethod httpMethod, String api, Object obj, Class<T> responseType) throws IOException, RecordNotFoundException {
//
//		// specify the host, protocol, and port
//		HttpHost target = new HttpHost(host, port, "http");
//
//		// specify the get request
//		HttpRequestBase request;
//		switch (httpMethod) {
//			case DELETE:
//				request = new HttpDelete(api);
//				break;
//			case PUT:
//				request = new HttpPut(api);
//				break;
//			case POST:
//				request = new HttpPost(api);
//				break;
//			default:
//				request = new HttpGet(api);
//				break;
//		}
//
//		if (obj != null && request instanceof HttpEntityEnclosingRequestBase) {
//			HttpEntity entity = new ByteArrayEntity(mapper.writeValueAsBytes(obj));
//			((HttpEntityEnclosingRequestBase) request).setEntity(entity);
//		}
//
//		logger.info(String.format("%s %s", httpMethod, api));
//
//		HttpResponse httpResponse = getHttpClient().execute(target, request);
//
//		int statusCode = httpResponse.getStatusLine().getStatusCode();
//
//		HttpEntity responseEntity = httpResponse.getEntity();
//
//		if (statusCode == HttpStatus.SC_OK) {
//			if (responseType != null) {
//				if (responseEntity != null) {
//					return mapper.readValue(responseEntity.getContent(), responseType);
//				}
//
//				throw new IOException("Empty response");
//			} else {
//				return null;
//			}
//		} else {
//			if (responseEntity != null) {
//				handleException(mapper.readValue(responseEntity.getContent(), HttpError.class));
//			}
//			throw new IOException(String.format("", statusCode));
//		}
//	}
//
//	private void handleException(HttpError e) throws RecordNotFoundException, IOException {
//		if (e.getCode() == 11) {
//			throw new RecordNotFoundException(e.getMessage());
//		}
//		throw new IOException(e.getMessage());
//	}
//}
