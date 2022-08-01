package com.winning.ods;

import com.alibaba.fastjson.JSON;
import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.winning.utils.CustomerDeserializationSchema;
import com.winning.sink.LINKAGE_RULE_LABTEST_ORDER_015_Sink;
import com.winning.utils.HikariUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;


/**
 * @author liuyi
 * @ClassName MysqlTwoPhaseCommit
 * @Description flink to mysql 两阶段提交代码
 * @date 2022/4/14 10:01
 * @Version 1.0
 */
public class LINKAGE_RULE_LABTEST_ORDER_015_Job {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //FlinkCDC接入到源端数据
        String mysql_source_file = "mysql_source.properties";
        String filePath;
        String osName = System.getProperties().getProperty("os.name");
        if (osName.toUpperCase().contains("WINDOWS"))
            filePath = Objects.requireNonNull(HikariUtil.class.getClassLoader().getResource(mysql_source_file)).getFile();
        else
            filePath = "/opt/module/realtime/myDB/" + mysql_source_file;//此地址使用linux服务器上面的绝对地址，根据地址自行修改
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(filePath);
        System.out.println("ckp_interval_millisecond:" + Long.parseLong(parameterTool.get("ckp_interval_millisecond")));
        //设置并行度,为了方便测试，查看消息的顺序，这里设置为1，可以更改为多并行度
        env.setParallelism(1);
        //checkpoint的设置
        //每隔10s进行启动一个检查点【设置checkpoint的周期】
        env.enableCheckpointing(Long.parseLong(parameterTool.get("ckp_interval_millisecond")));
        //设置模式为：exactly_one，仅一次语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
        //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        //同一时间只允许进行一次检查点
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置statebackend,将检查点保存在hdfs上面，默认保存在内存中。这里先保存到本地
//        env.setStateBackend(new FsStateBackend(statebackend_address));
        System.out.println("mysql_source_host:" + parameterTool.get("hostname"));
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname(parameterTool.get("hostname"))
                .port(Integer.parseInt(parameterTool.get("port")))
                .username(parameterTool.get("username"))
                .password(parameterTool.get("password"))
                .databaseList(parameterTool.get("databaseList"))
                .tableList(parameterTool.get("databaseList") + ".LINKAGE_RULE_LABTEST_ORDER")
                .deserializer(new CustomerDeserializationSchema())
                .startupOptions(StartupOptions.latest())  //initial()  latest
                .build();
        DataStreamSource<String> DSS_orders = env.addSource(sourceFunction);
        DSS_orders.print("DSS_orders");
        SingleOutputStreamOperator<String> DSS_orders2 = (SingleOutputStreamOperator<String>) DSS_orders.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return JSON.parseObject(s).getString("after");
            }
        });
        DSS_orders2.print("DSS_orders2");
        SingleOutputStreamOperator<Tuple1<String>> DSS_orders3 = DSS_orders2.map(
                        t -> Tuple1.of(
                                JSON.parseObject(t).toJSONString()
                        )
                )
                .returns(Types.TUPLE(Types.STRING));
        DSS_orders3.print("DSS_orders3");
        //数据传输到下游
        DSS_orders3.addSink(new LINKAGE_RULE_LABTEST_ORDER_015_Sink()).name("Sink_015");
        //触发执行
        env.execute("LINKAGE_RULE_LABTEST_ORDER_015_Job");
    }
}

