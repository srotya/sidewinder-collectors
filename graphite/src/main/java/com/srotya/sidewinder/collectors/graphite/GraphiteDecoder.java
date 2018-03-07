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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import com.srotya.sidewinder.core.rpc.Ack;
import com.srotya.sidewinder.core.rpc.Point;
import com.srotya.sidewinder.core.rpc.Point.Builder;
import com.srotya.sidewinder.core.rpc.SingleData;
import com.srotya.sidewinder.core.rpc.WriterServiceGrpc.WriterServiceStub;

import io.grpc.stub.StreamObserver;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

/**
 * http://graphite.readthedocs.io/en/latest/feeding-carbon.html#the-plaintext-protocol
 * metricpath metricvalue metrictimestamp
 * 
 * @author ambud
 */
public class GraphiteDecoder extends SimpleChannelInboundHandler<String> {

	private static final Logger logger = Logger.getLogger(GraphiteDecoder.class.getName());
	private String dbName;
	private Map<Long, SingleData> ledger;
	private StreamObserver<SingleData> writer;

	public GraphiteDecoder(String dbName, WriterServiceStub writerService) {
		this.dbName = dbName;
		ledger = new ConcurrentHashMap<>();
		StreamObserver<Ack> responseObserver = new StreamObserver<Ack>() {

			@Override
			public void onNext(Ack value) {
				ledger.remove(value.getMessageId());
			}

			@Override
			public void onError(Throwable t) {
				// TODO Retry
				t.printStackTrace();
			}

			@Override
			public void onCompleted() {
			}
		};
		writer = writerService.writeDataPointStream(responseObserver);
	}

	@Override
	public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
		ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
	}

	@Override
	protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
		logger.fine("Graphite input:" + msg);
		parseAndInsertDataPoints(dbName, msg, writer, ledger);
	}

	public static void parseAndInsertDataPoints(String dbName, String line, StreamObserver<SingleData> writer,
			Map<Long, SingleData> ledger) throws IOException {
		String[] parts = line.split("\\s+");
		if (parts.length != 3) {
			// invalid data point
			logger.fine("Ignoring bad metric:" + line);
			return;
		}
		String[] key = parts[0].split("\\.");
		String measurementName, valueFieldName;
		List<String> tags = new ArrayList<>();
		switch (key.length) {
		case 0:// invalid metric
		case 1:// invalid metric
		case 2:
			logger.fine("Ignoring bad metric:" + line);
			return;
		default:
			measurementName = key[key.length - 2];
			valueFieldName = key[key.length - 1];
			tags.add(key[0]);
			for (int i = 2; i < key.length - 1; i++) {
				tags.add(key[i]);
			}
			break;
		}
		long timestamp = Long.parseLong(parts[2]) * 1000;
		long messageId = System.nanoTime();
		Builder dp = Point.newBuilder().setDbName(dbName).setMeasurementName(measurementName)
				.setValueFieldName(valueFieldName).setTimestamp(timestamp).addAllTags(tags);
		if (parts[1].contains(".")) {
			double value = Double.parseDouble(parts[1]);
			dp.setFp(true).setValue(Double.doubleToLongBits(value));
			logger.info("Writing graphite metric (fp)" + dbName + "," + measurementName + "," + valueFieldName + ","
					+ tags + "," + timestamp + "," + value);

		} else {
			long value = Long.parseLong(parts[1]);
			dp.setFp(false).setValue(value);
			logger.info("Writing graphite metric (fp)" + dbName + "," + measurementName + "," + valueFieldName + ","
					+ tags + "," + timestamp + "," + value);
		}
		Point build = dp.build();
		SingleData point = SingleData.newBuilder().setMessageId(messageId).setPoint(build).build();
		ledger.put(messageId, point);
		writer.onNext(point);
	}

}
