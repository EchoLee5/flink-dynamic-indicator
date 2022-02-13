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

package com.hundsun.art.dynamicrules.functions;

import com.hundsun.art.dynamicrules.Indicator;
import com.hundsun.art.dynamicrules.IndicatorParser;
import com.hundsun.art.dynamicrules.JsonMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

@Slf4j
public class IndicatorDeserializer extends RichFlatMapFunction<String, Indicator> {

  private IndicatorParser parser;


  @Override
  public void open(Configuration parameters) throws Exception {
    super.open(parameters);
    parser = new IndicatorParser();
  }

  @Override
  public void flatMap(String value, Collector<Indicator> out) throws Exception {
    log.debug("{}", value);
    try {
      Indicator parsed = parser.fromString(value);
      out.collect(parsed);
    } catch (Exception e) {
      log.warn("Failed parsing indicator, dropping it:", e);
    }
  }
}
