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

package com.hundsun.art.dynamicrules.sources;

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource;
import com.hundsun.art.config.Config;
import com.hundsun.art.dynamicrules.KafkaUtils;
import com.hundsun.art.dynamicrules.Rule;
import com.hundsun.art.dynamicrules.RuleDeserializationSchema;
import com.hundsun.art.dynamicrules.functions.RuleDeserializer;

import java.io.IOException;
import java.util.Properties;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import static com.hundsun.art.config.Parameters.*;

public class RulesSource {

    private static final int RULES_STREAM_PARALLELISM = 1;

    public static SourceFunction<String> createRulesSource(Config config) throws IOException {

        String sourceType = config.get(RULES_SOURCE);
        Type rulesSourceType = Type.valueOf(sourceType.toUpperCase());

        switch (rulesSourceType) {
            case KAFKA:
                Properties kafkaProps = KafkaUtils.initConsumerProperties(config);
                String rulesTopic = config.get(RULES_TOPIC);
                FlinkKafkaConsumer<String> kafkaConsumer =
                        new FlinkKafkaConsumer<>(rulesTopic, new SimpleStringSchema(), kafkaProps);
                kafkaConsumer.setStartFromEarliest();
                return kafkaConsumer;
            case MYSQL:
                Properties dbzProperties = new Properties();
                dbzProperties.put("snapshot.mode", "schema_only");
                SourceFunction<String> mySqlSource = MySQLSource.<String>builder()
                        .hostname(config.get(MYSQL_HOST))
                        .port(config.get(MYSQL_PORT))
                        .username(config.get(MYSQL_USERNAME))
                        .password(config.get(MYSQL_PASSWORD))
                        .databaseList(config.get(RULES_DATABASE_NAME))
                        .tableList(config.get(RULES_DATABASE_NAME) + "." + config.get(RULES_TABLE_NAME))
//                .deserializer(new StringDebeziumDeserializationSchema())
                        .deserializer(new RuleDeserializationSchema())
                        .debeziumProperties(dbzProperties)
                        .build();
                return mySqlSource;
            case SOCKET:
                return new SocketTextStreamFunction("localhost", config.get(SOCKET_PORT), "\n", -1);
            default:
                throw new IllegalArgumentException(
                        "Source \"" + rulesSourceType + "\" unknown. Known values are:" + Type.values());
        }
    }

    public static DataStream<Rule> stringsStreamToRules(DataStream<String> ruleStrings) {
        return ruleStrings
                .flatMap(new RuleDeserializer())
                .name("Rule Deserialization")
                .setParallelism(RULES_STREAM_PARALLELISM)
                .assignTimestampsAndWatermarks(WatermarkStrategy
                        .forGenerator((ctx) -> new WatermarkGenerator<Rule>() {
                            @Override
                            public void onEvent(Rule event, long eventTimestamp, WatermarkOutput output) {
                            }

                            @Override
                            public void onPeriodicEmit(WatermarkOutput output) {
                                output.emitWatermark(new Watermark(Long.MAX_VALUE));
                            }
                        }));
    }

    public enum Type {
        KAFKA("Rules Source (Kafka)"),
        MYSQL("Rules Source (Mysql)"),
        SOCKET("Rules Source (Socket)");

        private String name;

        Type(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
