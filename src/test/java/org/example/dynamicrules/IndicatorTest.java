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

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class IndicatorTest {

  @SafeVarargs
  public static <T> List<T> lst(T... ts) {
    return Arrays.asList(ts);
  }

  @Test
  public void testIndicatorParsedPlain() throws Exception {
    String indicatorString = "esb_as_ProcBizFuncLibs,func_caption=AS_融资融券申报回报_参数获取,func_no=2275001,host=18.18.1.113,monitorKey=86cc74d02e0542a8a7528fd8a9f5c43a ave_time=0.0,createtime=1623285118247.0,cur_avgExcCount=0.0,cur_avgTime=0.5,cur_excCount=20.0,cur_excTime=10.0,cur_que_ave_time=\"0\",curr_queue_ave_time=\"0\",max_time=3.0,min_time=0.0,time_see=370.0,total=913.0 1623285118549000009";

    IndicatorParser indicatorParser = new IndicatorParser();
    Indicator indicator = indicatorParser.fromString(indicatorString);

    assertEquals("Measurement", "esb_as_ProcBizFuncLibs", indicator.getTableName());
    assertEquals("Timestamp", 1623285118549L, indicator.getEventTime());
//    assertEquals("Tags", 4, indicator.getTags().size());
    assertEquals("Fields", 16, indicator.getFields().size());

  }
}
