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

package com.hundsun.art.config;

import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.utils.ParameterTool;

public class Parameters {

    private final ParameterTool tool;

    public Parameters(ParameterTool tool) {
        this.tool = tool;
    }

    <T> T getOrDefault(Param<T> param) {
        if (!tool.has(param.getName())) {
            return param.getDefaultValue();
        }
        Object value;
        if (param.getType() == Integer.class) {
            value = tool.getInt(param.getName());
        } else if (param.getType() == Long.class) {
            value = tool.getLong(param.getName());
        } else if (param.getType() == Double.class) {
            value = tool.getDouble(param.getName());
        } else if (param.getType() == Boolean.class) {
            value = tool.getBoolean(param.getName());
        } else {
            value = tool.get(param.getName());
        }
        return param.getType().cast(value);
    }

    public static Parameters fromArgs(String[] args) {
        ParameterTool tool = ParameterTool.fromArgs(args);
        return new Parameters(tool);
    }

    // Kafka:
    public static final Param<String> KAFKA_HOST = Param.string("kafka.host", "localhost");
    public static final Param<Integer> KAFKA_PORT = Param.integer("kafka.port", 9092);

    // Elasticsearch
    public static final Param<String> ELASTICSEARCH_HOST = Param.string("elasticsearch.host", "localhost");
    public static final Param<Integer> ELASTICSEARCH_PORT = Param.integer("elasticsearch.port", 9200);
    public static final Param<Integer> BULK_FLUSH_MAX_ACTIONS = Param.integer("bulk.flush.max-actions", 100);
    public static final Param<Long> BULK_FLUSH_INTERVAL_MS = Param.bigint("bulk.flush.interval.ms", 3000L);

    // Mysql
    public static final Param<String> MYSQL_HOST = Param.string("mysql.host", "localhost");
    public static final Param<Integer> MYSQL_PORT = Param.integer("mysql.port", 33061);
    public static final Param<String> MYSQL_USERNAME = Param.string("mysql.username", "root");
    public static final Param<String> MYSQL_PASSWORD = Param.string("mysql.password", "123");
    public static final Param<String> RULES_DATABASE_NAME = Param.string("rules.database.name", "mysql");
    public static final Param<String> RULES_TABLE_NAME = Param.string("rules.table.name", "mysql");

    public static final Param<String> DATA_TOPIC = Param.string("data.topic", "liveindicators");
    public static final Param<String> RULES_TOPIC = Param.string("rules.topic", "rules");
    public static final Param<String> LATENCY_TOPIC = Param.string("latency.topic", "latency");
    public static final Param<String> RULES_EXPORT_TOPIC =
            Param.string("current.rules.topic", "current-rules");

    public static final Param<String> OFFSET = Param.string("offset", "latest");
    public static final Param<String> GROUP_ID = Param.string("group.id", "group");

    // Socket
    public static final Param<Integer> SOCKET_PORT = Param.integer("pubsub.rules.export", 9999);

    // General:
    //    source/sink types: kafka / pubsub / socket
    public static final Param<String> RULES_SOURCE = Param.string("rules.source", "MYSQL");
    public static final Param<String> TRANSACTIONS_SOURCE = Param.string("data.source", "KAFKA");
    public static final Param<String> RESULTS_SINK = Param.string("results.sink", "ELASTICSEARCH");
    public static final Param<String> LATENCY_SINK = Param.string("latency.sink", "STDOUT");
    public static final Param<String> RULES_EXPORT_SINK = Param.string("rules.export.sink", "STDOUT");

    public static final Param<Integer> RECORDS_PER_SECOND = Param.integer("records.per.second", 2);

    public static final Param<Boolean> LOCAL_EXECUTION = Param.bool("local", false);

    public static final Param<Integer> SOURCE_PARALLELISM = Param.integer("source.parallelism", 1);
    public static final Param<Integer> SOURCE_PARSE_PARALLELISM = Param.integer("source.parse.parallelism", 8);
    public static final Param<Integer> DYNAMIC_KEY_BY_PARALLELISM = Param.integer("dynamic.key-by.parallelism", 8);
    public static final Param<Integer> DYNAMIC_AGG_PARALLELISM = Param.integer("dynamic.agg.parallelism", 8);
    public static final Param<Integer> SINK_PARALLELISM = Param.integer("sink.parallelism", 8);

    public static final Param<Boolean> ENABLE_CHECKPOINTS = Param.bool("checkpoints", false);

    public static final Param<Integer> CHECKPOINT_INTERVAL =
            Param.integer("checkpoint.interval", 60_000_0);
    public static final Param<Integer> MIN_PAUSE_BETWEEN_CHECKPOINTS =
            Param.integer("min.pause.btwn.checkpoints", 10000);
    public static final Param<Integer> OUT_OF_ORDERNESS = Param.integer("out-of.orderdness", 500);
    public static final Param<Integer> IDLE_TIMEOUT = Param.integer("idle.timeout", 5000);

    public static final Param<Integer> STATE_TTL_MINUTES = Param.integer("state.ttl.minutes", 1440);

    public static final Param<Boolean> LOOKUP_ENABLE = Param.bool("lookup.enable", true);
    public static final Param<String> LOOKUP_TABLE_NAME = Param.string("lookup.table.name", "mysql");
    public static final Param<String> LOOKUP_DATABASE_NAME = Param.string("lookup.database.name", "mysql");
    public static final Param<String> LOOKUP_TABLE_JOIN_KEYS = Param.string("lookup.table.join-keys", "");
    public static final Param<String> LOOKUP_TABLE_EXTEND_FIELDS = Param.string("lookup.table.extend-fields", "");
    public static final Param<Long> LOOKUP_CACHE_MAX_SIZE = Param.bigint("lookup.cache.max-size", 1000L);
    public static final Param<Long> LOOKUP_CACHE_EXPIRE_MS = Param.bigint("lookup.cache.expire-ms", 10000L);

    //  List<Param> list = Arrays.asList(new String[]{"foo", "bar"});

    public static final List<Param<String>> STRING_PARAMS =
            Arrays.asList(
                    KAFKA_HOST,
                    ELASTICSEARCH_HOST,
                    MYSQL_HOST,
                    MYSQL_USERNAME,
                    MYSQL_PASSWORD,
                    RULES_DATABASE_NAME,
                    RULES_TABLE_NAME,
                    DATA_TOPIC,
                    RULES_TOPIC,
                    LATENCY_TOPIC,
                    RULES_EXPORT_TOPIC,
                    OFFSET,
                    GROUP_ID,
                    RULES_SOURCE,
                    TRANSACTIONS_SOURCE,
                    RESULTS_SINK,
                    LATENCY_SINK,
                    RULES_EXPORT_SINK,
                    LOOKUP_TABLE_NAME,
                    LOOKUP_DATABASE_NAME,
                    LOOKUP_TABLE_JOIN_KEYS,
                    LOOKUP_TABLE_EXTEND_FIELDS
                    );

    public static final List<Param<Integer>> INT_PARAMS =
            Arrays.asList(
                    KAFKA_PORT,
                    ELASTICSEARCH_PORT,
                    MYSQL_PORT,
                    BULK_FLUSH_MAX_ACTIONS,
                    SOCKET_PORT,
                    RECORDS_PER_SECOND,
                    SOURCE_PARALLELISM,
                    SOURCE_PARSE_PARALLELISM,
                    DYNAMIC_KEY_BY_PARALLELISM,
                    DYNAMIC_AGG_PARALLELISM,
                    SINK_PARALLELISM,
                    CHECKPOINT_INTERVAL,
                    MIN_PAUSE_BETWEEN_CHECKPOINTS,
                    OUT_OF_ORDERNESS,
                    IDLE_TIMEOUT,
                    STATE_TTL_MINUTES);

  public static final List<Param<Long>> LONG_PARAMS =
          Arrays.asList(
                  BULK_FLUSH_INTERVAL_MS,
                  LOOKUP_CACHE_MAX_SIZE,
                  LOOKUP_CACHE_EXPIRE_MS);

    public static final List<Param<Boolean>> BOOL_PARAMS =
            Arrays.asList(LOCAL_EXECUTION,
                    ENABLE_CHECKPOINTS,
                    LOOKUP_ENABLE);
}
