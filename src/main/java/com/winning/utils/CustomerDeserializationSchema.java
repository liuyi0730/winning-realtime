package com.winning.utils;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.List;

public class CustomerDeserializationSchema implements DebeziumDeserializationSchema<String> {
    /**
     * {
     * "db":"",
     * "tableName":"",
     * "before":{"id":"1001","name":""...},
     * "after":{"id":"1001","name":"","op":"","binlog_start_time":"",...}
     * "op":""
     * }
     */
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {
        //创建JSON对象用于封装结果数据
        JSONObject result = new JSONObject();
        //获取库名&表名
        String topic = sourceRecord.topic();
        String[] fields = topic.split("\\.");
        result.put("db", fields[1]);
        result.put("tableName", fields[2]);
        //获取before数据
        Struct value = (Struct) sourceRecord.value();
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (before != null) {
            //获取列信息
            Schema schema = before.schema();
            List<Field> fieldList = schema.fields();
            for (Field field : fieldList) {
                beforeJson.put(field.name(), before.get(field));
            }
        }
        result.put("before", beforeJson);
        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
//        添加binlog操作时间
        Struct source = value.getStruct("source");
        String binlog_start_time = source.get("ts_ms").toString();
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        //获取after数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (after != null) {
            //获取列信息
            Schema schema = after.schema();
            List<Field> fieldList = schema.fields();
            for (Field field : fieldList) {
                afterJson.put(field.name(), after.get(field));
            }
            afterJson.put("op", operation.toString());
            afterJson.put("binlog_start_time", binlog_start_time);
        }
        result.put("after", afterJson);
        //将删除操作也纳入到after中
        if (operation.toString().equals("DELETE")) {
            JSONObject DelBeforeJson = new JSONObject();
            if (before != null) {
                //获取列信息
                Schema schema = before.schema();
                List<Field> fieldList = schema.fields();
                for (Field field : fieldList) {
                    DelBeforeJson.put(field.name(), before.get(field));
                }
                DelBeforeJson.put("op", operation.toString());
                DelBeforeJson.put("binlog_start_time", binlog_start_time);
            }
            result.put("after", DelBeforeJson);
        }
        ;
        //获取操作类型
        result.put("op", operation);
        //输出数据
        collector.collect(result.toJSONString());
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
