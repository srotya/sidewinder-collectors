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
package com.srotya.sidewinder.collectors.graphite;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import com.srotya.sidewinder.core.rpc.WriterServiceGrpc;
import com.srotya.sidewinder.core.rpc.WriterServiceGrpc.WriterServiceStub;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/**
 * @author ambud
 */
public class GraphiteProxy {

	public static void main(String[] args) throws Exception {
		Map<String, String> conf = new HashMap<>();
		extractProperties(args, conf);
		String address = conf.getOrDefault("sidewinder.grpc.host", "localhost");
		int port = Integer.parseInt(conf.getOrDefault("sidewinder.grpc.port", "9928"));
		if (!telnetCheck(address, port)) {
			throw new Exception("Sidewinder server not reachable on the configured port");
		}
		final ManagedChannel channel = ManagedChannelBuilder.forAddress(address, port).build();
		WriterServiceStub writerService = WriterServiceGrpc.newStub(channel);
		final GraphiteTCPServer tcp = new GraphiteTCPServer(conf, writerService);
		tcp.start();
		Runtime.getRuntime().addShutdownHook(new Thread("shutdown-thread") {
			@Override
			public void run() {
				try {
					channel.shutdown();
					tcp.stop();
				} catch (Exception e) {
					System.err.println(e.getMessage());
				}
			}
		});
	}

	public static boolean telnetCheck(String address, int port) {
		try {
			Socket sc = new Socket(address, port);
			sc.close();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	private static void extractProperties(String[] args, Map<String, String> conf)
			throws IOException, FileNotFoundException {
		Properties props = new Properties();
		props.load(new FileInputStream(args[0]));
		for (Entry<Object, Object> entry : props.entrySet()) {
			conf.put(entry.getValue().toString(), entry.getValue().toString());
		}
	}

}
