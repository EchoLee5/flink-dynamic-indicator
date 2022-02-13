package org.example.dynamicrules;

import org.example.dynamicrules.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.AggregateFunction;

import java.math.BigDecimal;
import java.util.Map;

/**
 * @author lisen
 * @desc
 * @date Created in 2021/11/2 13:14
 */
@Slf4j
public class SumAggregateFunction implements AggregateFunction<Keyed<Indicator, String, Rule>, Result, Result> {
    @Override
    public Result createAccumulator() {
        Result r = new Result<>();
        r.setTriggeringValue(BigDecimal.valueOf(0));
        return r;
    }

    @Override
    public Result add(Keyed<Indicator, String, Rule> value, Result accumulator) {
        BigDecimal triggeringValue = (BigDecimal) accumulator.getTriggeringValue();
        BigDecimal aggregatedValue = BigDecimal.valueOf(0);
        try {
            Map<String, String> fields = FieldsExtractor.<Map>getByKeyAs("fields", value.getWrapped());

            String aggName = "";
            aggregatedValue = BigDecimal.valueOf(Double.parseDouble(fields.get(aggName)));
        } catch (Exception e) {
            log.info("Parse aggregated value error {}", e.getMessage());
        }

        accumulator.setKey(value.getKey());
        accumulator.setRuleId(value.getId().getRuleId());
        accumulator.setTriggeringEvent(value.getWrapped());
        accumulator.setViolatedRule(value.getId());
        aggregatedValue = triggeringValue.add(aggregatedValue);
        accumulator.setTriggeringValue(aggregatedValue);

        return accumulator;
    }

    @Override
    public Result getResult(Result accumulator) {
        return accumulator;
    }

    @Override
    public Result merge(Result a, Result b) {
        BigDecimal aggregatedValue1 = (BigDecimal) a.getTriggeringValue();
        BigDecimal aggregatedValue2 = (BigDecimal) b.getTriggeringValue();
        b.setTriggeringValue(aggregatedValue1.add(aggregatedValue2));
        return b;
    }
}
