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

import static org.junit.Assert.assertEquals;

import org.example.dynamicrules.Rule.RuleState;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

public class RuleParserTest {

    @SafeVarargs
    public static <T> List<T> lst(T... ts) {
        return Arrays.asList(ts);
    }

    @Test
    public void testRuleParsedPlain() throws Exception {
        String ruleString1 = "1,esb_as_ProcBizFuncLibs,active,host&monitorKey,cur_excCount:sum&cur_excCount:max&cur_excCount:min,10";
        RuleParser ruleParser = new RuleParser();
        Rule rule1 = ruleParser.fromString(ruleString1);

        assertEquals("ID incorrect", 1, (int) rule1.getRuleId());
        assertEquals("Rule tableName incorrect", "esb_as_ProcBizFuncLibs", rule1.getTableName());
        Assert.assertEquals("Rule state incorrect", RuleState.ACTIVE, rule1.getRuleState());
        assertEquals("Key names incorrect", lst("host", "monitorKey"), rule1.getGroupingKeyNames());
        assertEquals("Cumulative key and type incorrect", 3, rule1.getAggFields().size());
        assertEquals("Window incorrect", 10, (int) rule1.getWindowMinutes());
    }

    @Test
    public void testRuleParsedJson() throws Exception {
        String ruleString1 =
                "{\n"
                        + "  \"ruleId\": 1,\n"
                        + "  \"ruleState\": \"ACTIVE\",\n"
                        + "  \"groupingKeyNames\": [\"taxiId\", \"driverId\"],\n"
                        + "  \"unique\": [],\n"
                        + "  \"aggregateFieldName\": \"totalFare\",\n"
                        + "  \"aggregatorFunctionType\": \"SUM\",\n"
                        + "  \"limitOperatorType\": \"GREATER\",\n"
                        + "  \"limit\": 50,\n"
                        + "  \"windowMinutes\": 20\n"
                        + "}";

        RuleParser ruleParser = new RuleParser();
        Rule rule1 = ruleParser.fromString(ruleString1);

        assertEquals("ID incorrect", 1, (int) rule1.getRuleId());
        Assert.assertEquals("Rule state incorrect", RuleState.ACTIVE, rule1.getRuleState());
        assertEquals("Key names incorrect", lst("taxiId", "driverId"), rule1.getGroupingKeyNames());
//        assertEquals("Cumulative key incorrect", "totalFare", rule1.getAggregateFieldName());
//        Assert.assertEquals(
//                "Aggregator function incorrect",
//                AggregatorFunctionType.SUM,
//                rule1.getAggregatorFunctionType());
//        Assert.assertEquals(
//                "Limit operator incorrect", LimitOperatorType.GREATER, rule1.getLimitOperatorType());
//        assertEquals("Limit incorrect", BigDecimal.valueOf(50), rule1.getLimit());
        assertEquals("Window incorrect", 20, (int) rule1.getWindowMinutes());
    }
}
