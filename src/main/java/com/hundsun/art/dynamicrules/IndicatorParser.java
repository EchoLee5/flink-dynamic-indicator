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

import java.io.IOException;
import java.util.*;


public class IndicatorParser {

    public Indicator fromString(String line) throws IOException {
        return parsePlain(line);
    }


    private static Indicator parsePlain(String indicatorString) throws IOException {
        List<String> tokens = Arrays.asList(indicatorString.trim()
                .split(" (?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)",-1));
        if (tokens.size() != 3) {
            throw new IOException("Invalid indicator (wrong number of tokens): " + indicatorString);
        }

        Iterator<String> iter = tokens.iterator();
        Indicator indicator = new Indicator();

        String measurementAndTagStr = iter.next();
        String fieldStr = iter.next();
        String timestampStr = iter.next();

        List<String> measurementAndTags = Arrays.asList(measurementAndTagStr.split(","));
        if (measurementAndTags.size() < 1) {
            throw new IOException("Invalid measurementAndTags (wrong number of measurementAndTags): " + measurementAndTags);
        }

        String measurement = measurementAndTags.get(0);
//        Map<String, String> tags = new HashMap<>();
        Map<String, String> fields = new HashMap<>();

        List<String> tagList = measurementAndTags.subList(1, measurementAndTags.size());
        for (String tag : tagList) {
            List<String> kv = getKeyValues(tag);
            fields.put(kv.get(0), kv.get(1));
        }

        List<String> field = Arrays.asList(fieldStr.split(","));
        for (String f : field) {
            List<String> kv = getKeyValues(f);
            fields.put(kv.get(0), kv.get(1));
        }

        indicator.setTableName(measurement);
//        indicator.setTags(tags);
        indicator.setFields(fields);
        indicator.setEventTime(Long.parseLong(timestampStr) / (1000 * 1000));

        return indicator;
    }


    private static List<String> getKeyValues(String keyValueStr) throws IOException {
        String[] kv = keyValueStr.split("=");
        if (kv.length != 2) {
            throw new IOException("Invalid kv (wrong number of kv): " + kv);
        }

        return Arrays.asList(kv);
    }
}
