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

package com.hundsun.art.dynamicrules;

import com.hundsun.art.dynamicrules.Rule.AggregatorFunctionType;
import com.hundsun.art.dynamicrules.Rule.LimitOperatorType;
import com.hundsun.art.dynamicrules.Rule.RuleState;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.*;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

public class RuleParser {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public Rule fromString(String line) throws IOException {
        if (line.length() > 0 && '{' == line.charAt(0)) {
            return parseJson(line);
        } else {
            return parsePlain(line);
        }
    }

    private Rule parseJson(String ruleString) throws IOException {
        return objectMapper.readValue(ruleString, Rule.class);
    }

    private static Rule parsePlain(String ruleString) throws IOException {
        List<String> tokens = Arrays.asList(ruleString.split(","));
        if (tokens.size() != 6) {
            throw new IOException("Invalid rule (wrong number of tokens): " + ruleString);
        }

        Iterator<String> iter = tokens.iterator();
        Rule rule = new Rule();

        rule.setRuleId(Integer.parseInt(iter.next()));
        rule.setTableName(iter.next());
        rule.setRuleState(Rule.RuleState.valueOf(iter.next().toUpperCase()));
        rule.setGroupingKeyNames(getNames(iter.next()));
//        rule.setUnique(getNames(iter.next()));
        rule.setAggFields(getAggFields(iter.next()));
//        rule.setAggregateFieldName(stripBrackets(iter.next()));
//        rule.setAggregatorFunctionType(
//                AggregatorFunctionType.valueOf(stripBrackets(iter.next()).toUpperCase()));
//        rule.setLimitOperatorType(Rule.LimitOperatorType.fromString(iter.next()));
//        rule.setLimit(new BigDecimal(iter.next()));
        rule.setWindowMinutes(Integer.parseInt(iter.next()));

        return rule;
    }

    private static String stripBrackets(String expression) {
        return expression.replaceAll("[()]", "");
    }

    private static List<String> getNames(String keyNamesString) {
//        String keyNamesString = expression.replaceAll("[()]", "");
        if (!"".equals(keyNamesString)) {
            String[] tokens = keyNamesString.split("&", -1);
            return Arrays.asList(tokens);
        } else {
            return new ArrayList<>();
        }
    }

    private static List<Tuple2<String, AggregatorFunctionType>> getAggFields(String expression) {
        List<Tuple2<String, AggregatorFunctionType>> aggFieldList = new ArrayList<>();
        List<String> aggFields = getNames(expression);
        for (String aggField : aggFields) {
            if (!"".equals(aggField)) {
                String[] tokens = aggField.split(":", -1);
                String aggName = tokens[0];
                AggregatorFunctionType aggFuncType = AggregatorFunctionType.valueOf(tokens[1].toUpperCase());
                aggFieldList.add(Tuple2.of(aggName, aggFuncType));
            }
        }
        return aggFieldList;
    }
}
