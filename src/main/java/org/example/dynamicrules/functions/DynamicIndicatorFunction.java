package org.example.dynamicrules.functions;

import org.example.dynamicrules.*;
import org.example.dynamicrules.sources.BoundedRuleLoader;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.accumulators.SimpleAccumulator;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MeterView;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.example.config.Parameters;
import org.example.dynamicrules.*;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * @author lisen
 * @desc 实现动态聚合结果输出
 * @date Created in 2021/11/3 19:15
 */
@Slf4j
public class DynamicIndicatorFunction extends KeyedBroadcastProcessFunction<
        String, Keyed<Indicator, String, Integer>, Rule, Result<Indicator, Map<String, BigDecimal>>> {

    private static final String COUNT = "COUNT_FLINK";
    private static final String COUNT_WITH_RESET = "COUNT_WITH_RESET_FLINK";

    private static int WIDEST_RULE_KEY = Integer.MIN_VALUE;
    private static int CLEAR_STATE_COMMAND_KEY = Integer.MIN_VALUE + 1;

    private transient MapState<Integer, Result<Indicator, Map<String, BigDecimal>>> resultsState;

    private transient MapState<Integer, Map<String, SimpleAccumulator<BigDecimal>>> accumulatorState;

    // Map<规则ID, 输出结果>
    private MapStateDescriptor<Integer, Result<Indicator, Map<String, BigDecimal>>> resultsStateDescriptor = new MapStateDescriptor<>(
            "resultsState",
            TypeInformation.of(new TypeHint<Integer>() {
            }),
            TypeInformation.of(new TypeHint<Result<Indicator, Map<String, BigDecimal>>>() {
            })
    );

    // Map<规则ID, Map<分组字段+聚合类型, 聚合函数>>
    private MapStateDescriptor<Integer, Map<String, SimpleAccumulator<BigDecimal>>> accumulatorStateDescriptor = new MapStateDescriptor<>(
            "accumulatorState",
            TypeInformation.of(new TypeHint<Integer>() {
            }),
            TypeInformation.of(new TypeHint<Map<String, SimpleAccumulator<BigDecimal>>>() {
            })
    );

    // 设置状态清理TTL，默认1天
    StateTtlConfig ttlConfig;

    private Meter alertMeter;

    private Map<Integer, Rule> rulesCache = new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        Map<String, String> globalJobParameters = getRuntimeContext().getExecutionConfig()
                .getGlobalJobParameters().toMap();

        long ttl = Long.parseLong(globalJobParameters.get(Parameters.STATE_TTL_MINUTES.getName()));
        ttlConfig = StateTtlConfig
                .newBuilder(Time.days(ttl))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .cleanupInRocksdbCompactFilter(1000)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
        accumulatorStateDescriptor.enableTimeToLive(ttlConfig);

        resultsState = getRuntimeContext().getMapState(resultsStateDescriptor);
        accumulatorState = getRuntimeContext().getMapState(accumulatorStateDescriptor);

        alertMeter = new MeterView(60);
        getRuntimeContext().getMetricGroup().meter("alertsPerSecond", alertMeter);

        // 加载全量规则
        rulesCache.putAll(new BoundedRuleLoader(globalJobParameters).load());
    }

    @Override
    public void processElement(Keyed<Indicator, String, Integer> value, ReadOnlyContext ctx,
                               Collector<Result<Indicator, Map<String, BigDecimal>>> out) throws Exception {
        long currentEventTime = value.getWrapped().getEventTime();

        Rule rule = rulesCache.get(value.getId());
        if (noRuleAvailable(rule)) {
            log.error("Rule with ID {} does not exist", value.getId());
            return;
        }

        TimeWindow window = WindowHelper.assignWindows(currentEventTime, rule.getWindowMillis());

        long ingestionTime = value.getWrapped().getIngestionTimestamp();
        ctx.output(RulesEvaluator.Descriptors.latencySinkTag, System.currentTimeMillis() - ingestionTime);

        if (rule.getRuleState() == Rule.RuleState.ACTIVE) {
            long collectTime = window.maxTimestamp();
            log.debug("Current key is {}, Register event time timer {}", ctx.getCurrentKey(), collectTime);
            ctx.timerService().registerEventTimeTimer(collectTime);

            // 聚合结果并存入状态
            aggregateValuesInState(window, value, rule);

            if (COUNT_WITH_RESET.equals(rule.getAggFields().get(0))) {
                evictAllStateElements();
            }
            alertMeter.markEvent();
        }

    }

    @Override
    public void processBroadcastElement(Rule rule, Context ctx, Collector<Result<Indicator, Map<String, BigDecimal>>> out) throws Exception {
        log.info("{}", rule);
        ProcessingUtils.handleRulesCache(rule, rulesCache);
    }

    /**
     * 当watermark到达窗口结束时间时触发输出
     *
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(final long timestamp, final OnTimerContext ctx,
                        final Collector<Result<Indicator, Map<String, BigDecimal>>> out)
            throws Exception {
        log.debug("Current key is {}, Current time is {}, and try clean state", ctx.getCurrentKey(), timestamp);

        Iterator<Integer> iterator = resultsState.keys().iterator();
        while (iterator.hasNext()) {
            Integer key = iterator.next();

            Result result = resultsState.get(key);
            out.collect(result);
            iterator.remove();
        }
    }

    /**
     * 对当前记录和之前的结果做聚合操作
     *
     * @param window
     * @param value
     * @param rule
     * @throws Exception
     */
    private void aggregateValuesInState(
            TimeWindow window, Keyed<Indicator, String, Integer> value, Rule rule) throws Exception {
        Map<String, SimpleAccumulator<BigDecimal>> buffAccumulatorValue = accumulatorState.get(rule.getRuleId());
        if (buffAccumulatorValue == null) {
            buffAccumulatorValue = new HashMap<>();
        }

        for (Tuple2<String, Rule.AggregatorFunctionType> aggField : rule.getAggFields()) {
            String fieldNameType = aggField.f0 + "-" + aggField.f1;
            BigDecimal aggregatedValue = BigDecimal.valueOf(0);
            Map<String, String> fields = FieldsExtractor.<Map>getByKeyAs("fields", value.getWrapped());
            try {
                // 当前值
                String curValue = fields.get(aggField.f0);
                if (StringUtils.isNoneEmpty(curValue) && curValue.contains("\"")) {
                    curValue = curValue.replaceAll("\"", "");
                }
                aggregatedValue = BigDecimal.valueOf(Double.parseDouble(curValue));
            } catch (Exception e) {
                log.info("Parse aggregated value error {}", e.getMessage());
            }

            SimpleAccumulator<BigDecimal> accumulator = buffAccumulatorValue.get(fieldNameType);
            if (accumulator == null) {
                accumulator = RuleHelper.getAggregator(aggField.f1);
            }

            if (aggField.f1 == Rule.AggregatorFunctionType.COUNT) {
                accumulator.add(BigDecimal.ONE);
            } else {
                accumulator.add(aggregatedValue);
            }

            buffAccumulatorValue.put(fieldNameType, accumulator);
        }

        accumulatorState.put(rule.getRuleId(), buffAccumulatorValue);

        Map<String, BigDecimal> buffValue = new HashMap<>();
        for (Map.Entry<String, SimpleAccumulator<BigDecimal>> entry : buffAccumulatorValue.entrySet()) {
            buffValue.put(entry.getKey(), entry.getValue().getLocalValue());
        }

        Result<Indicator, Map<String, BigDecimal>> result = new Result<>(
                rule.getRuleId(), rule, value.getKey(), window.getStart(), window.getEnd(),
                value.getWrapped(), buffValue);
        resultsState.put(rule.getRuleId(), result);
    }

    private boolean noRuleAvailable(Rule rule) {
        // This could happen if the BroadcastState in this CoProcessFunction was updated after it was
        // updated and used in `DynamicKeyFunction`
        if (rule == null) {
            return true;
        }
        return false;
    }

    private void handleControlCommand(
            Rule command, BroadcastState<Integer, Rule> rulesState, Context ctx) throws Exception {
        Rule.ControlType controlType = command.getControlType();
        switch (controlType) {
            case EXPORT_RULES_CURRENT:
                for (Map.Entry<Integer, Rule> entry : rulesState.entries()) {
                    ctx.output(RulesEvaluator.Descriptors.currentRulesSinkTag, entry.getValue());
                }
                break;
            case CLEAR_STATE_ALL:
                ctx.applyToKeyedState(resultsStateDescriptor, (key, state) -> state.clear());
                break;
            case CLEAR_STATE_ALL_STOP:
                rulesState.remove(CLEAR_STATE_COMMAND_KEY);
                break;
            case DELETE_RULES_ALL:
                Iterator<Map.Entry<Integer, Rule>> entriesIterator = rulesState.iterator();
                while (entriesIterator.hasNext()) {
                    Map.Entry<Integer, Rule> ruleEntry = entriesIterator.next();
                    rulesState.remove(ruleEntry.getKey());
                    log.info("Removed Rule {}", ruleEntry.getValue());
                }
                break;
        }
    }

    private void evictAllStateElements() {
        try {
            resultsState.clear();
            accumulatorState.clear();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
