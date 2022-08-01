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
public class EXAMINATION_ITEM_054_Sink extends TwoPhaseCommitSinkFunction<Tuple1<String>, HikariUtil, Void> { //表名替换
    private static final Logger log = LoggerFactory.getLogger(EXAMINATION_ITEM_054_Sink.class);//表名替换

    public EXAMINATION_ITEM_054_Sink() {//表名替换
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
        String KeyID_01 = jsonObject.getString("EXAM_ITEM_ID"); //
        String KeyID_02 = jsonObject.getString("HOSPITAL_SOID"); //
        String upd_sql = "update examination_item set end_time=? " +    //
                "where EXAM_ITEM_ID =? and HOSPITAL_SOID =? and end_time='3000-12-31 00:00:00' "; //
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
        String insert_sql = "insert into examination_item " +
                "(EXAM_ITEM_ID,EXAM_ITEM_CATEGORY_ID,CS_ID,EXAM_ITEM_NAME,EXAM_ITEM_NO,CALCULATION_CARDINAL,CORRESPONDING_DIRECTION_CODE,NAME_PREFIX,NAME_SUFFIX,HOSPITAL_SOID,IS_DEL,CREATED_AT,MODIFIED_AT,STORAGE_MEDIA_ALLOWED_FLAG,IMAGE_REBUILD_METHOD_CODE,SEQ_NO,BODY_SITE_CODE,BODY_SIDE_CODE,REDIATION_DIRECTION_CODE,REDIATION_EXAM_TYPE_CODE,REDIATION_STRENGTH_CODE,PINYIN,WUBI,GENDER_LIMIT_CODE,MODIFY_QTY_ALLOWED_FLAG,ENABLED_FLAG,EXAM_NOTES,SOURCE_EXAM_ITEM_ID,start_time,physical_delete) " +
                "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";//改为表的字段总数-1
        log.info("====insert_sql执行SQL:===", insert_sql);
        System.out.println("====insert_sql执行SQL:===" + insert_sql);
        PreparedStatement insert_ps = HikariUtil.getconn().prepareStatement(insert_sql);
        insert_ps.setString(1, KeyID_01);
        insert_ps.setString(2, jsonObject.getString("EXAM_ITEM_CATEGORY_ID"));
        insert_ps.setString(3, jsonObject.getString("CS_ID"));
        insert_ps.setString(4, jsonObject.getString("EXAM_ITEM_NAME"));
        insert_ps.setString(5, jsonObject.getString("EXAM_ITEM_NO"));
        insert_ps.setString(6, jsonObject.getString("CALCULATION_CARDINAL"));
        insert_ps.setString(7, jsonObject.getString("CORRESPONDING_DIRECTION_CODE"));
        insert_ps.setString(8, jsonObject.getString("NAME_PREFIX"));
        insert_ps.setString(9, jsonObject.getString("NAME_SUFFIX"));
        insert_ps.setString(10, KeyID_02);
        insert_ps.setString(11, jsonObject.getString("IS_DEL"));
        insert_ps.setTimestamp(12, jsonObject.getTimestamp("CREATED_AT"));
        insert_ps.setTimestamp(13, jsonObject.getTimestamp("MODIFIED_AT"));
        insert_ps.setString(14, jsonObject.getString("STORAGE_MEDIA_ALLOWED_FLAG"));
        insert_ps.setString(15, jsonObject.getString("IMAGE_REBUILD_METHOD_CODE"));
        insert_ps.setString(16, jsonObject.getString("SEQ_NO"));
        insert_ps.setString(17, jsonObject.getString("BODY_SITE_CODE"));
        insert_ps.setString(18, jsonObject.getString("BODY_SIDE_CODE"));
        insert_ps.setString(19, jsonObject.getString("REDIATION_DIRECTION_CODE"));
        insert_ps.setString(20, jsonObject.getString("REDIATION_EXAM_TYPE_CODE"));
        insert_ps.setString(21, jsonObject.getString("REDIATION_STRENGTH_CODE"));
        insert_ps.setString(22, jsonObject.getString("PINYIN"));
        insert_ps.setString(23, jsonObject.getString("WUBI"));
        insert_ps.setString(24, jsonObject.getString("GENDER_LIMIT_CODE"));
        insert_ps.setString(25, jsonObject.getString("MODIFY_QTY_ALLOWED_FLAG"));
        insert_ps.setString(26, jsonObject.getString("ENABLED_FLAG"));
        insert_ps.setString(27, jsonObject.getString("EXAM_NOTES"));
        insert_ps.setString(28, jsonObject.getString("SOURCE_EXAM_ITEM_ID"));
        insert_ps.setTimestamp(29, new Timestamp(jsonObject.getTimestamp("binlog_start_time").getTime()));//改为表的字段数-2
        String op = jsonObject.getString("op");
        if (op.equals("DELETE")) {
            insert_ps.setInt(30, 1);//当有delete操作时:设置is_del为0  //
        } else {
            insert_ps.setInt(30, 0); //默认为0  //
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