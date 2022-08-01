package com.winning.sink;

import com.alibaba.fastjson.JSONObject;
import com.winning.utils.HikariUtil;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.Timestamp;

/**
 * @author liuyi
 * @ClassName MysqlSinkPlus
 * @Description TODO
 * @date 2022/07/06 10:03
 * @Version 1.0
 */
public class CLINICAL_SERVICE_CARE_049_Sink extends TwoPhaseCommitSinkFunction<Tuple1<String>, HikariUtil, Void> { //表名替换
    private static final Logger log = LoggerFactory.getLogger(CLINICAL_SERVICE_CARE_049_Sink.class);//表名替换

    public CLINICAL_SERVICE_CARE_049_Sink() {//表名替换
        super(new KryoSerializer<>(HikariUtil.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
    }

    /**
     * 执行数据库入库操作  task初始化的时候调用
     *
     * @param HikariUtil
     * @param tuple
     * @param context
     * @throws Exception
     */
    @Override
    protected void invoke(HikariUtil HikariUtil, Tuple1<String> tuple, Context context) throws Exception {
        log.info("start invoke...");
        System.out.println("start invoke...");
        String json = tuple.f0;//得到json串
        JSONObject jsonObject = JSONObject.parseObject(json);
        //主键设置，可能是多个字段组合成主键
		//------------begin---添加字段-----------
        String KeyID_01 = jsonObject.getString("CS_ID"); //
        String KeyID_02 = jsonObject.getString("HOSPITAL_SOID"); //
        String upd_sql = "update clinical_service_care set end_time=? " +    //
                "where CS_ID =? and HOSPITAL_SOID =? and end_time='3000-12-31 00:00:00' "; //
        System.out.println("upd_sql==" + upd_sql);
        PreparedStatement upd_ps = HikariUtil.getconn().prepareStatement(upd_sql);
        upd_ps.setTimestamp(1, new Timestamp(jsonObject.getTimestamp("binlog_start_time").getTime() ));
        upd_ps.setString(2, KeyID_01);
        upd_ps.setString(3, KeyID_02);
        if (upd_ps != null) {
            String sqlStr = upd_ps.toString().substring(upd_ps.toString().indexOf(":") + 2);
            log.error("upd_ps执行的SQL语句:{}", sqlStr);
            System.out.println("upd_ps执行的SQL语句sqlStr:" + sqlStr);
        }
        String insert_sql = "insert into clinical_service_care " +
                "(CS_ID,CARE_GROUP_CODE,ORDER_PERIOD_CODE,ORDER_PERIOD_MODIFIABLE_FLAG,INTRA_GROUP_EXCLUSIVE_FLAG,HOSPITAL_SOID,IS_DEL,CREATED_AT,MODIFIED_AT,start_time,physical_delete) " +
                "values (?,?,?,?,?,?,?,?,?,?,?)";//改为表的字段总数-1
        log.info("====insert_sql执行SQL:===", insert_sql);
        System.out.println("====insert_sql执行SQL:===" + insert_sql);
        PreparedStatement insert_ps = HikariUtil.getconn().prepareStatement(insert_sql);
        insert_ps.setString(1, KeyID_01);
        insert_ps.setString(2, jsonObject.getString("CARE_GROUP_CODE"));
        insert_ps.setString(3, jsonObject.getString("ORDER_PERIOD_CODE"));
        insert_ps.setString(4, jsonObject.getString("ORDER_PERIOD_MODIFIABLE_FLAG"));
        insert_ps.setString(5, jsonObject.getString("INTRA_GROUP_EXCLUSIVE_FLAG"));
        insert_ps.setString(6, KeyID_02);
        insert_ps.setString(7, jsonObject.getString("IS_DEL"));
        insert_ps.setTimestamp(8, jsonObject.getTimestamp("CREATED_AT"));
        insert_ps.setTimestamp(9, jsonObject.getTimestamp("MODIFIED_AT"));
        insert_ps.setTimestamp(10, new Timestamp(jsonObject.getTimestamp("binlog_start_time").getTime() ));//改为表的字段数-2
        String op = jsonObject.getString("op");
        if (op.equals("DELETE")) {
            insert_ps.setInt(11, 1);//当有delete操作时:设置is_del为0  //
        } else {
            insert_ps.setInt(11, 0); //默认为0  //
        }
//------------end---添加字段-----------
        if (insert_ps != null) {
            String sqlStr = insert_ps.toString().substring(insert_ps.toString().indexOf(":") + 2);
            log.error("insert_ps执行的SQL语句:{}", sqlStr);
            System.out.println("insert_ps执行的SQL语句sqlStr:" + sqlStr);
        }
        //执行
        System.out.println("执行sql语句开始！");
        upd_ps.execute();
        insert_ps.execute();
        System.out.println("执行sql语句完成！");
    }

    /**
     * 获取连接，开启手动提交事物（getConnection方法中）
     *
     * @return
     * @throws Exception
     */
    @Override
    protected HikariUtil beginTransaction() throws Exception {
        log.info("start beginTransaction.......");
        System.out.println("start beginTransaction.......");
        return new HikariUtil();
    }

    /**
     * 预提交，这里预提交的逻辑在invoke方法中
     *
     * @param HikariUtil
     * @throws Exception
     */
    @Override
    protected void preCommit(HikariUtil HikariUtil) throws Exception {
        log.info("start preCommit...");
        System.out.println("start preCommit...");
    }

    /**
     * 如果invoke方法执行正常，则提交事务
     *
     * @param HikariUtil
     */
    @Override
    protected void commit(HikariUtil HikariUtil) {
        log.info("start commit...");
        System.out.println("start commit...");
        HikariUtil.commit();
    }

    /**
     * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
     *
     * @param HikariUtil
     */
    @Override
    protected void abort(HikariUtil HikariUtil) {
        log.info("start abort rollback...");
        System.out.println("start abort rollback...");
        HikariUtil.rollback();
    }
}