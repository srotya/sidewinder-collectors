/**
 * Copyright 2017 Ambud Sharma
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * 		http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.sidewinder.collectors.flume;

import java.io.IOException;
import java.util.Map;

import org.apache.flume.Channel;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.Transaction;
import org.apache.flume.sink.AbstractSink;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.Configurable;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import com.google.common.collect.ImmutableMap;

/**
 * @Author Suma Cherukuri
 */
public class FlumeSink  extends AbstractSink implements Configurable {
	
	private String FIELD_PROP = "fields";
	private String TAG_PROP = "tags";
	private String MEASUREMENT_PROP = "measurement";
	private static String DB_NAME_PROP ="database";
	
	private static final String TAG_STARTS_WITH = "tag_";
	private static final String FIELD_STARTS_WITH = "field_";
	private static final String DELIMITER = ",";
	
	StringBuilder URL = new StringBuilder("http://localhost:8080");
	private String database, measurement, tags, fields;
	private CloseableHttpClient client;
	
	public void configure(Context context) {
		ImmutableMap<String, String> subProperties = context.getSubProperties("");
		database = subProperties.get(DB_NAME_PROP);
		measurement = subProperties.get(MEASUREMENT_PROP);
		tags = subProperties.get(TAG_PROP);
		fields = subProperties.get(FIELD_PROP);
	}
	
	@Override
	public void start() {
		// Initialize the connection to the external repository (e.g. HDFS) that
		// this Sink will forward Events to ..
		try {
			URL.append("/http?db=").append(database);
			if(client == null) {
				client = buildClient(URL.toString(), 5000, 5000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static CloseableHttpClient buildClient(String baseURL, int connectTimeout, int requestTimeout)
			throws Exception {
		HttpClientBuilder clientBuilder = HttpClients.custom();
		RequestConfig config = RequestConfig.custom().setConnectTimeout(connectTimeout)
				.setConnectionRequestTimeout(requestTimeout).build();

		return clientBuilder.setDefaultRequestConfig(config).build();
	}

	@Override
	public void stop () {
		try {
			client.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public Status process() throws EventDeliveryException {
		Status status = null;
		// Start transaction
		Channel ch = getChannel();
		Transaction txn = ch.getTransaction();
		txn.begin();
		try {
			// This try clause includes whatever Channel operations you want to do
			Event event = ch.take();
			String data = constructSidewinderData(event);
			sendEvent(data);
			txn.commit();
			status = Status.READY;
		} catch (Throwable t) {
			txn.rollback();
			// Log exception, handle individual exceptions as needed
			status = Status.BACKOFF;
			// re-throw all Errors
			if (t instanceof Error) {
				throw (Error)t;
			}
		}
		return status;
	}

	private void sendEvent(String data) throws ClientProtocolException, IOException {
		HttpPost db = new HttpPost(URL.toString()); // create the HTTP POST request
		db.setEntity(new StringEntity(data)); // Set the payload
		CloseableHttpResponse execute = client.execute(db); // Execute request and capture the response
		System.out.println(execute.getStatusLine()); // Print the Response
		execute.close(); // Close the connection
	}
	
	public String constructSidewinderData(/*String event_fields, String event_tags, 
			String event_measurement,*/ Event event) {
		StringBuilder builder = new StringBuilder();
		builder.append(event.getHeaders().get(MEASUREMENT_PROP));
		builder.append(",");
		String FIELD_DELIMITER = "";
		Map<String, String> eventHeaders = event.getHeaders();
		for(String key : eventHeaders.keySet())
			if(key.startsWith(TAG_STARTS_WITH)) {
				builder.append(FIELD_DELIMITER);
				FIELD_DELIMITER = ",";
				builder.append(key.substring(TAG_STARTS_WITH.length()) + "=" + event.getHeaders().get(key) );
				
			}
		builder.append(" ");
		
		FIELD_DELIMITER = "";
		for(String key : eventHeaders.keySet()) {
			if(key.startsWith(FIELD_STARTS_WITH)) {
				builder.append(FIELD_DELIMITER);
				FIELD_DELIMITER = ",";
				builder.append(key.substring(FIELD_STARTS_WITH.length()) + "=" + event.getHeaders().get(key));
			}
		}
		builder.append(" ");
		builder.append(System.currentTimeMillis());
		return builder.toString();
	}

	@Override
	public RequestConfig getConfig() {
		return null;
	}
}