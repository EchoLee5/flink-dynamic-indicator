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


import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Utilities for dynamic keys extraction by field name.
 */
public class KeysExtractor {

    /**
     * Extracts and concatenates field values by names.
     *
     * @param rule     list of field names
     * @param object   target for values extraction
     */
    public static String getKey(String tagName, Rule rule, Object object, TimeWindow window)
            throws NoSuchFieldException, IllegalAccessException {
        List<String> keyNames = rule.getGroupingKeyNames();
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        if (keyNames.size() > 0) {
            Iterator<String> it = keyNames.iterator();
            appendValue(sb, tagName, object, it.next());

            while (it.hasNext()) {
                sb.append("-");
                appendValue(sb, tagName, object, it.next());
            }

            sb.append("-").append(window.getStart())
                    .append("-")
                    .append(window.getEnd());
        }
        sb.append("}");
        return sb.toString();
    }

    private static void appendValue(StringBuilder sb, String tagName, Object object, String fieldName)
            throws IllegalAccessException, NoSuchFieldException {
        Map<String, String> tags = FieldsExtractor.<Map<String, String>>getByKeyAs(tagName, object);
        sb.append(tags.get(fieldName));
    }
}
