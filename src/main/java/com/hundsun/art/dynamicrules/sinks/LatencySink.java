/*
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

package com.hundsun.art.dynamicrules.sinks;

import static com.hundsun.art.config.Parameters.LATENCY_SINK;
import static com.hundsun.art.config.Parameters.LATENCY_TOPIC;

import com.hundsun.art.config.Config;
import com.hundsun.art.dynamicrules.KafkaUtils;
import java.io.IOException;
import java.util.Properties;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

public class LatencySink {

  public static SinkFunction<String> createLatencySink(Config config) throws IOException {

    String latencySink = config.get(LATENCY_SINK);
    Type latencySinkType = Type.valueOf(latencySink.toUpperCase());

    switch (latencySinkType) {
      case KAFKA:
        Properties kafkaProps = KafkaUtils.initProducerProperties(config);
        String latencyTopic = config.get(LATENCY_TOPIC);
        return new FlinkKafkaProducer<>(latencyTopic, new SimpleStringSchema(), kafkaProps);

      case STDOUT:
        return new PrintSinkFunction<>(true);
      default:
        throw new IllegalArgumentException(
            "Source \"" + latencySinkType + "\" unknown. Known values are:" + Type.values());
    }
  }

  public enum Type {
    KAFKA("Latency Sink (Kafka)"),
    STDOUT("Latency Sink (Std. Out)");

    private String name;

    Type(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }
}
