package com.hundsun.art.dynamicrules;

import lombok.*;

import java.util.Map;

/**
 * @author lisen
 * @desc 原始指标
 * @date Created in 2021/10/28 17:29
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class Indicator implements TimestampAssignable<Long> {
    // 表名
    public String tableName;
    // 字段名称+字段值
    public Map<String, String> fields;
    // 事件时间
    public long eventTime;
    // 摄入时间
    private Long ingestionTimestamp;

    @Override
    public void assignIngestionTimestamp(Long timestamp) {
        this.ingestionTimestamp = timestamp;
    }
}
