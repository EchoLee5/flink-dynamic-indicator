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

import org.example.dynamicrules.Rule;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;

public class ProcessingUtils {

  public static void handleRuleBroadcast(Rule rule, BroadcastState<Integer, Rule> broadcastState)
      throws Exception {
    switch (rule.getRuleState()) {
      case ACTIVE:
      case PAUSE:
        broadcastState.put(rule.getRuleId(), rule);
        break;
      case DELETE:
        broadcastState.remove(rule.getRuleId());
        break;
    }
  }

  public static void handleRulesCache(Rule rule, Map<Integer, Rule> cacheRules) {
    switch (rule.getRuleState()) {
      case ACTIVE:
      case PAUSE:
        cacheRules.put(rule.getRuleId(), rule);
        break;
      case DELETE:
        cacheRules.remove(rule.getRuleId());
        break;
    }
  }

  static <K, V> Set<V> addToStateValuesSet(MapState<K, Set<V>> mapState, K key, V value)
      throws Exception {

    Set<V> valuesSet = mapState.get(key);

    if (valuesSet != null) {
      valuesSet.add(value);
    } else {
      valuesSet = new HashSet<>();
      valuesSet.add(value);
    }
    mapState.put(key, valuesSet);
    return valuesSet;
  }
}
