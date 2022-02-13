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

package org.example.dynamicrules;

import org.example.config.Config;
import org.example.dynamicrules.functions.*;
import org.example.dynamicrules.functions.DynamicIndicatorFunction;
import org.example.dynamicrules.functions.DynamicKeyFunction;
import org.example.dynamicrules.functions.LookupFunction;
import org.example.dynamicrules.sinks.CurrentRulesSink;
import org.example.dynamicrules.sinks.ResultSink;
import org.example.dynamicrules.sources.RulesSource;
import org.example.dynamicrules.sources.IndicatorsSource;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.OutputTag;

import static org.example.config.Parameters.*;

@Slf4j
public class RulesEvaluator {

    private Config config;

    RulesEvaluator(Config config) {
        this.config = config;
    }

    public void run() throws Exception {

        RulesSource.Type rulesSourceType = getRulesSourceType();

        boolean isLocal = config.get(LOCAL_EXECUTION);
        boolean enableCheckpoints = config.get(ENABLE_CHECKPOINTS);
        int checkpointsInterval = config.get(CHECKPOINT_INTERVAL);
        int minPauseBtwnCheckpoints = config.get(CHECKPOINT_INTERVAL);

        // Environment setup
        StreamExecutionEnvironment env = configureStreamExecutionEnvironment(rulesSourceType, isLocal);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(config.getConfig()));

        if (enableCheckpoints) {
            env.enableCheckpointing(checkpointsInterval);
            env.getCheckpointConfig().setMinPauseBetweenCheckpoints(minPauseBtwnCheckpoints);
        }

        // Streams setup
        DataStream<Rule> rulesUpdateStream = getRulesUpdateStream(env);
        DataStream<Indicator> indicators = getIndicatorStream(env);

        BroadcastStream<Rule> rulesStream = rulesUpdateStream.broadcast(Descriptors.rulesDescriptor);

        // lookup
        boolean lookupEnable = config.get(LOOKUP_ENABLE);
        if (lookupEnable) {
            indicators = indicators.map(new LookupFunction())
                    .returns(Indicator.class)
                    .setParallelism(config.get(SOURCE_PARSE_PARALLELISM));
        }

        // Processing pipeline setup
        DataStream<Result<Indicator, Map<String, BigDecimal>>> result =
                indicators
                        .keyBy(value -> value.getTableName())
                        .connect(rulesStream)
                        .process(new DynamicKeyFunction())
                        .setParallelism(config.get(DYNAMIC_KEY_BY_PARALLELISM))
                        .uid("DynamicKeyFunction")
                        .name("Dynamic Partitioning Function")
                        .keyBy(keyed -> keyed.getKey())
                        .connect(rulesStream)
                        .process(new DynamicIndicatorFunction())
                        .setParallelism(config.get(DYNAMIC_AGG_PARALLELISM))
                        .name("Dynamic Rule Evaluation Function");

        result.addSink(ResultSink.createResultsSink(config))
                .setParallelism(config.get(SINK_PARALLELISM))
                .name("Elasticsearch Sink");

        DataStream<Rule> currentRules =
                ((SingleOutputStreamOperator<Result<Indicator, Map<String, BigDecimal>>>) result)
                        .getSideOutput(Descriptors.currentRulesSinkTag);
        DataStream<String> currentRulesJson = CurrentRulesSink.rulesStreamToJson(currentRules);

        currentRulesJson.print();

        env.execute("Indicator Aggregate Engine");
    }

    private DataStream<Indicator> getIndicatorStream(StreamExecutionEnvironment env) {
        // Data stream setup
        SourceFunction<String> indicatorSource = IndicatorsSource.createIndicatorSource(config);
        int sourceParallelism = config.get(SOURCE_PARALLELISM);
        DataStream<String> indicatorsStringsStream =
                env.addSource(indicatorSource)
                        .name("Indicator Source")
                        .setParallelism(sourceParallelism);
        DataStream<Indicator> indicatorsStream =
                IndicatorsSource.stringsStreamToIndicators(config, indicatorsStringsStream);
        long maxOutOfOrderness = config.get(OUT_OF_ORDERNESS);
        long idleTimeout = config.get(IDLE_TIMEOUT);
        return indicatorsStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Indicator>forGenerator((ctx) -> new BoundedOutOfOrdernessWatermarksWithIdleness<>(
                                Duration.ofMillis(maxOutOfOrderness), Duration.ofMillis(idleTimeout)))
                                .withTimestampAssigner((event, timestamp) -> event.getEventTime())
                );
    }

    private DataStream<Rule> getRulesUpdateStream(StreamExecutionEnvironment env) throws IOException {

        RulesSource.Type rulesSourceEnumType = getRulesSourceType();

        SourceFunction<String> rulesSource = RulesSource.createRulesSource(config);
        DataStream<String> rulesStrings =
                env.addSource(rulesSource).name(rulesSourceEnumType.getName()).setParallelism(1);
        return RulesSource.stringsStreamToRules(rulesStrings);
    }

    private RulesSource.Type getRulesSourceType() {
        String rulesSource = config.get(RULES_SOURCE);
        return RulesSource.Type.valueOf(rulesSource.toUpperCase());
    }

    private StreamExecutionEnvironment configureStreamExecutionEnvironment(
            RulesSource.Type rulesSourceEnumType, boolean isLocal) {
        Configuration flinkConfig = new Configuration();
//    flinkConfig.setBoolean(ConfigConstants.LOCAL_START_WEBSERVER, true);

        StreamExecutionEnvironment env =
                isLocal
                        ? StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(flinkConfig)
                        : StreamExecutionEnvironment.getExecutionEnvironment();

//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getCheckpointConfig().setCheckpointInterval(config.get(CHECKPOINT_INTERVAL));
        env.getCheckpointConfig()
                .setMinPauseBetweenCheckpoints(config.get(MIN_PAUSE_BETWEEN_CHECKPOINTS));

        configureRestartStrategy(env, rulesSourceEnumType);
        return env;
    }

    private void configureRestartStrategy(
            StreamExecutionEnvironment env, RulesSource.Type rulesSourceEnumType) {
        switch (rulesSourceEnumType) {
            case SOCKET:
                env.setRestartStrategy(
                        RestartStrategies.fixedDelayRestart(
                                10, org.apache.flink.api.common.time.Time.of(10, TimeUnit.SECONDS)));
                break;
            case KAFKA:
                // Default - unlimited restart strategy.
                //        env.setRestartStrategy(RestartStrategies.noRestart());
        }
    }

    public static class Descriptors {
        public static final MapStateDescriptor<Integer, Rule> rulesDescriptor =
                new MapStateDescriptor<>(
                        "rules", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(Rule.class));

        public static final OutputTag<String> demoSinkTag = new OutputTag<String>("demo-sink") {
        };
        public static final OutputTag<Long> latencySinkTag = new OutputTag<Long>("latency-sink") {
        };
        public static final OutputTag<Rule> currentRulesSinkTag =
                new OutputTag<Rule>("current-rules-sink") {
                };
    }
}
