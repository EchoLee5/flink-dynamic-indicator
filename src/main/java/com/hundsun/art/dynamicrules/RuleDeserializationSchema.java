package com.hundsun.art.dynamicrules;

import com.alibaba.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

/**
 * @author lisen
 * @desc
 * @date Created in 2021/11/16 18:22
 */
public class RuleDeserializationSchema implements DebeziumDeserializationSchema<String> {
    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        Envelope.Operation op = Envelope.operationFor(record);
        Struct value = (Struct) record.value();
        Schema valueSchema = record.valueSchema();
        if (op == Envelope.Operation.CREATE || op == Envelope.Operation.READ) {
            String insert = extractAfterRow(value, valueSchema);
            out.collect(insert);
        } else if (op == Envelope.Operation.DELETE) {
            // do nothing
        } else {
            // only collect after row
            String after = extractAfterRow(value, valueSchema);
            out.collect(after);
        }
    }

    private String extractAfterRow(Struct value, Schema valueSchema) throws Exception {
        Schema afterSchema = valueSchema.field(Envelope.FieldName.AFTER).schema();
        Struct after = value.getStruct(Envelope.FieldName.AFTER);
        return convert(after, afterSchema);
    }

    private String extractBeforeRow(Struct value, Schema valueSchema) throws Exception {
        Schema afterSchema = valueSchema.field(Envelope.FieldName.BEFORE).schema();
        Struct after = value.getStruct(Envelope.FieldName.BEFORE);
        return convert(after, afterSchema);
    }

    private String convert(Struct value, Schema valueSchema) {
        StringBuilder builder = new StringBuilder();
            for (Field field : valueSchema.fields()) {
            builder.append(value.get(field)).append(",");
        }
        builder.deleteCharAt(builder.length() - 1);

        return builder.toString();
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
