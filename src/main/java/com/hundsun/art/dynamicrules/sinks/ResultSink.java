package com.hundsun.art.dynamicrules.sinks;

import com.hundsun.art.config.Config;
import com.hundsun.art.config.Parameters;
import com.hundsun.art.dynamicrules.*;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.hundsun.art.config.Parameters.*;

/**
 * @author lisen
 * @desc
 * @date Created in 2021/10/29 22:15
 */
@Slf4j
public class ResultSink {
    public static SinkFunction<Result<Indicator, Map<String, BigDecimal>>> createResultsSink(Config config) throws IOException {
        String resultSink = config.get(RESULTS_SINK);
        Type resultsSinkType = Type.valueOf(resultSink.toUpperCase());

        switch (resultsSinkType) {
            case ELASTICSEARCH:
                return createElasticsearchSink(config);
            case STDOUT:
                return new PrintSinkFunction<>(true);
            default:
                throw new IllegalArgumentException(
                        "Sink \"" + resultsSinkType + "\" unknown. Known values are:" + Type.values());
        }


    }

    public static ElasticsearchSink<Result<Indicator, Map<String, BigDecimal>>> createElasticsearchSink(Config config) {
        List<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost(config.get(Parameters.ELASTICSEARCH_HOST),
                config.get(Parameters.ELASTICSEARCH_PORT), "http"));

        ElasticsearchSink.Builder<Result<Indicator, Map<String, BigDecimal>>> esSinkBuilder = new ElasticsearchSink.Builder<>(
                httpHosts,
                new ElasticsearchSinkFunction<Result<Indicator, Map<String, BigDecimal>>>() {

                    public UpdateRequest createUpdateRequest(Result<Indicator, Map<String, BigDecimal>> element) throws IOException {

                        Indicator indicator = element.getTriggeringEvent();
                        Rule rule = element.getViolatedRule();

                        XContentBuilder builder = XContentFactory.jsonBuilder();
                        builder.startObject();

                        for (String groupingKeyName : rule.getGroupingKeyNames()) {
                            builder.field(groupingKeyName, indicator.getFields().get(groupingKeyName));
                        }

                        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");
                        builder.field("startTime", dateFormat.format(element.getStartTime()))
                                .field("endTime", dateFormat.format(element.getEndTime()));

                        Map<String, BigDecimal> value = element.getTriggeringValue();
                        for (Map.Entry<String, BigDecimal> entry: value.entrySet()){
                            builder.field(entry.getKey(), entry.getValue());
                        }

                        builder.endObject();

                        return new UpdateRequest(indicator.tableName.toLowerCase(), element.getKey())
                                .type("indicator")
                                .doc(builder)
                                .docAsUpsert(true);
                    }

                    @SneakyThrows
                    @Override
                    public void process(Result element, RuntimeContext ctx, RequestIndexer indexer) {
                        indexer.add(createUpdateRequest(element));
                    }
                }
        );

        esSinkBuilder.setBulkFlushMaxActions(config.get(BULK_FLUSH_MAX_ACTIONS));
        esSinkBuilder.setBulkFlushInterval(config.get(BULK_FLUSH_INTERVAL_MS));

        return esSinkBuilder.build();
    }

    public enum Type {
        ELASTICSEARCH("Latency Sink (Elasticsearch)"),
        STDOUT("Latency Sink (Std. Out)");

        private String name;

        Type(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }
    }
}
