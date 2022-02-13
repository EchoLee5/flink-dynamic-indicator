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

package org.example.dynamicrules.functions;

import static org.example.dynamicrules.functions.ProcessingUtils.handleRulesCache;

import org.example.dynamicrules.*;
import org.example.dynamicrules.*;
import org.example.dynamicrules.Rule.ControlType;

import java.util.*;
import java.util.Map.Entry;

import org.example.dynamicrules.sources.BoundedRuleLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Implements dynamic data partitioning based on a set of broadcasted rules.
 */
@Slf4j
public class DynamicKeyFunction
        extends KeyedBroadcastProcessFunction<String, Indicator, Rule, Keyed<Indicator, String, Integer>> {

    private RuleCounterGauge ruleCounterGauge;
    private Map<Integer, Rule> rulesCache = new HashMap<>();

    @Override
    public void open(Configuration parameters) {
        ruleCounterGauge = new RuleCounterGauge();
        getRuntimeContext().getMetricGroup().gauge("numberOfActiveRules", ruleCounterGauge);
        Map<String, String> globalJobParameters = getRuntimeContext().getExecutionConfig()
                .getGlobalJobParameters().toMap();
        // 加载全量规则
        rulesCache.putAll(new BoundedRuleLoader(globalJobParameters).load());
    }

    @Override
    public void processElement(
            Indicator event, ReadOnlyContext ctx, Collector<Keyed<Indicator, String, Integer>> out)
            throws Exception {
        forkEventForEachGroupingKey(event, out);
    }

    private void forkEventForEachGroupingKey(
            Indicator event, Collector<Keyed<Indicator, String, Integer>> out) throws Exception {
        int ruleCounter = 0;
        for (Map.Entry<Integer, Rule> entry : rulesCache.entrySet()) {
            ruleCounter++;
            final Rule rule = entry.getValue();

            if (!checkRule(rule, event)) {
                log.debug("Grouping field is not full be alone to event fields.");
                continue;
            }

            long currentEventTime = event.getEventTime();
            TimeWindow window = WindowHelper.assignWindows(currentEventTime, rule.getWindowMillis());
            // {分组字段-windowStart-windowEnd}
            String key = KeysExtractor.getKey("fields", rule, event, window);

            out.collect(
                    new Keyed<>(event, key, rule.getRuleId()));
        }

        ruleCounterGauge.setValue(ruleCounter);
    }

    public boolean checkRule(Rule rule, Indicator indicator) {
        if (!rule.getTableName().equals(indicator.getTableName())) {
            return false;
        }

        Map<String, String> fields = indicator.getFields();
        for (Tuple2<String, Rule.AggregatorFunctionType> aggField : rule.getAggFields()) {
            if (!fields.containsKey(aggField.f0)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void processBroadcastElement(
            Rule rule, Context ctx, Collector<Keyed<Indicator, String, Integer>> out) throws Exception {
        log.info("{}", rule);
        handleRulesCache(rule, rulesCache);
    }

    private void handleControlCommand(
            ControlType controlType, BroadcastState<Integer, Rule> rulesState) throws Exception {
        switch (controlType) {
            case DELETE_RULES_ALL:
                Iterator<Entry<Integer, Rule>> entriesIterator = rulesState.iterator();
                while (entriesIterator.hasNext()) {
                    Entry<Integer, Rule> ruleEntry = entriesIterator.next();
                    rulesState.remove(ruleEntry.getKey());
                    log.info("Removed Rule {}", ruleEntry.getValue());
                }
                break;
        }
    }

    private static class RuleCounterGauge implements Gauge<Integer> {

        private int value = 0;

        public void setValue(int value) {
            this.value = value;
        }

        @Override
        public Integer getValue() {
            return value;
        }
    }
}
