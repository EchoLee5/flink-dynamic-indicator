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

package org.example.dynamicrules.sources;

import org.example.config.Config;
import org.example.dynamicrules.Indicator;
import org.example.dynamicrules.KafkaUtils;
import org.example.dynamicrules.functions.*;

import java.util.Properties;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.example.config.Parameters;
import org.example.dynamicrules.functions.*;

public class IndicatorsSource {

    public static SourceFunction<String> createIndicatorSource(Config config) {

        String sourceType = config.get(Parameters.TRANSACTIONS_SOURCE);
        Type transactionsSourceType =
                Type.valueOf(sourceType.toUpperCase());

        int transactionsPerSecond = config.get(Parameters.RECORDS_PER_SECOND);

        switch (transactionsSourceType) {
            case KAFKA:
                Properties kafkaProps = KafkaUtils.initConsumerProperties(config);
                String transactionsTopic = config.get(Parameters.DATA_TOPIC);
                FlinkKafkaConsumer<String> kafkaConsumer =
                        new FlinkKafkaConsumer<>(transactionsTopic, new SimpleStringSchema(), kafkaProps);
                kafkaConsumer.setStartFromEarliest();
                return kafkaConsumer;
            case GENERATOR:
                return new JsonGeneratorWrapper<>(new TransactionsGenerator(transactionsPerSecond));

            default:
                return new IndicatorsGenerator(transactionsPerSecond);
        }
    }

    public static DataStream<Indicator> stringsStreamToIndicators(Config config,
                                                                  DataStream<String> indicatorStrings) {
        return indicatorStrings
                .flatMap(new IndicatorDeserializer())
                .returns(Indicator.class)
                .setParallelism(config.get(Parameters.SOURCE_PARSE_PARALLELISM))
                .flatMap(new TimeStamper<Indicator>())
                .returns(Indicator.class)
                .setParallelism(config.get(Parameters.SOURCE_PARSE_PARALLELISM))
                .name("Indicators Deserialization");
    }

    public enum Type {
        GENERATOR("Transactions Source (generated locally)"),
        KAFKA("Transactions Source (Kafka)"),
        FIX("Indicator Source"),
        FILE("Indicator File Source");

        private String name;

        Type(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
