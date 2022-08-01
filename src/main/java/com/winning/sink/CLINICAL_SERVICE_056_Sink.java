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
public class CLINICAL_SERVICE_056_Sink extends TwoPhaseCommitSinkFunction<Tuple1<String>, HikariUtil, Void> { //表名替换
    private static final Logger log = LoggerFactory.getLogger(CLINICAL_SERVICE_056_Sink.class);//表名替换

    public CLINICAL_SERVICE_056_Sink() {//表名替换
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
        String upd_sql = "update clinical_service set end_time=? " +    //
                "where CS_ID =? and HOSPITAL_SOID =? and end_time='3000-12-31 00:00:00' "; //
        System.out.println("upd_sql==" + upd_sql);
        PreparedStatement upd_ps = HikariUtil.getconn().prepareStatement(upd_sql);
        upd_ps.setTimestamp(1, new Timestamp(jsonObject.getTimestamp("binlog_start_time").getTime()));
        upd_ps.setString(2, KeyID_01);
        upd_ps.setString(3, KeyID_02);
        if (upd_ps != null) {
            String sqlStr = upd_ps.toString().substring(upd_ps.toString().indexOf(":") + 2);
            log.error("upd_ps执行的SQL语句:{}", sqlStr);
            System.out.println("upd_ps执行的SQL语句sqlStr:" + sqlStr);
        }
        String insert_sql = "insert into clinical_service " +
                "(CS_ID,CS_CATEGORY_ID,CS_CODE,CS_NO,CS_NAME,CS_NAME_2ND,CS_DESC,CUSTOM_NAME,PINYIN,WUBI,SHORTCUT,CS_TYPE_CODE,CS_STATUS,MEMO,ORDER_FLAG,SUBSTANCE_FLAG,CS_SET_FLAG,ASSESSMENT_FLAG,URGENT_ALLOWED_FLAG,DEFAULT_PRESCRIBED_QTY,DEFAULT_PRESCRIBED_UNIT_CODE,REF_PRICE,PRICE_DESC,NOTES,HOSPITAL_SOID,IS_DEL,CREATED_AT,MODIFIED_AT,CONTRAST_AGENT_FLAG,MEDIA_SUBJECT_FLAG,CONCEPT_ID,MODIFY_FREQ_ALLOWED_FLAG,CS_PACKAGE_FLAG,CLI_ORDER_FLOW_RULE_GROUP_ID,SOURCE_CS_ID,start_time,physical_delete) " +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";//改为表的字段总数-1
        log.info("====insert_sql执行SQL:===", insert_sql);
        System.out.println("====insert_sql执行SQL:===" + insert_sql);
        PreparedStatement insert_ps = HikariUtil.getconn().prepareStatement(insert_sql);
        insert_ps.setString(1, KeyID_01);
        insert_ps.setString(2, jsonObject.getString("CS_CATEGORY_ID"));
        insert_ps.setString(3, jsonObject.getString("CS_CODE"));
        insert_ps.setString(4, jsonObject.getString("CS_NO"));
        insert_ps.setString(5, jsonObject.getString("CS_NAME"));
        insert_ps.setString(6, jsonObject.getString("CS_NAME_2ND"));
        insert_ps.setString(7, jsonObject.getString("CS_DESC"));
        insert_ps.setString(8, jsonObject.getString("CUSTOM_NAME"));
        insert_ps.setString(9, jsonObject.getString("PINYIN"));
        insert_ps.setString(10, jsonObject.getString("WUBI"));
        insert_ps.setString(11, jsonObject.getString("SHORTCUT"));
        insert_ps.setString(12, jsonObject.getString("CS_TYPE_CODE"));
        insert_ps.setString(13, jsonObject.getString("CS_STATUS"));
        insert_ps.setString(14, jsonObject.getString("MEMO"));
        insert_ps.setString(15, jsonObject.getString("ORDER_FLAG"));
        insert_ps.setString(16, jsonObject.getString("SUBSTANCE_FLAG"));
        insert_ps.setString(17, jsonObject.getString("CS_SET_FLAG"));
        insert_ps.setString(18, jsonObject.getString("ASSESSMENT_FLAG"));
        insert_ps.setString(19, jsonObject.getString("URGENT_ALLOWED_FLAG"));
        insert_ps.setString(20, jsonObject.getString("DEFAULT_PRESCRIBED_QTY"));
        insert_ps.setString(21, jsonObject.getString("DEFAULT_PRESCRIBED_UNIT_CODE"));
        insert_ps.setString(22, jsonObject.getString("REF_PRICE"));
        insert_ps.setString(23, jsonObject.getString("PRICE_DESC"));
        insert_ps.setString(24, jsonObject.getString("NOTES"));
        insert_ps.setString(25, KeyID_02);
        insert_ps.setString(26, jsonObject.getString("IS_DEL"));
        insert_ps.setTimestamp(27, jsonObject.getTimestamp("CREATED_AT"));
        insert_ps.setTimestamp(28, jsonObject.getTimestamp("MODIFIED_AT"));
        insert_ps.setString(29, jsonObject.getString("CONTRAST_AGENT_FLAG"));
        insert_ps.setString(30, jsonObject.getString("MEDIA_SUBJECT_FLAG"));
        insert_ps.setString(31, jsonObject.getString("CONCEPT_ID"));
        insert_ps.setString(32, jsonObject.getString("MODIFY_FREQ_ALLOWED_FLAG"));
        insert_ps.setString(33, jsonObject.getString("CS_PACKAGE_FLAG"));
        insert_ps.setString(34, jsonObject.getString("CLI_ORDER_FLOW_RULE_GROUP_ID"));
        insert_ps.setString(35, jsonObject.getString("SOURCE_CS_ID"));
        insert_ps.setTimestamp(36, new Timestamp(jsonObject.getTimestamp("binlog_start_time").getTime()));//改为表的字段数-2
        String op = jsonObject.getString("op");
        if (op.equals("DELETE")) {
            insert_ps.setInt(37, 1);//当有delete操作时:设置is_del为0  //
        } else {
            insert_ps.setInt(37, 0); //默认为0  //
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