package com.winning.utils;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.Properties;

/**
 * @author liuyi
 * @ClassName HikariUtil
 * @Description JDBC工具类，使用HikariCP做连接池
 * @date 2022/4/18 14:21
 * @Version 1.0
 */
public class HikariUtil {
    private static final Logger log = LoggerFactory.getLogger(HikariUtil.class);
    private final static HikariUtil HIKARI_CP_UTI = new HikariUtil();
    private static Properties properties = null;
    private static HikariDataSource dataSource = null;
    private static transient Connection conn = null;

    //1.单例模式中,应该创建私有构造方法
    public HikariUtil() {
    }

    /**
     * 1.配置和获取数据库连接配置信息
     * 2.扩展HikariCP功能,进行配置
     * 3.获取数据库连接,提供对外获取数据库资源的方法
     */
    private void initConfig() throws IOException {
        String osName = System.getProperties().getProperty("os.name");
        String filePath;
        String mysql_target_file = "mysql_target.properties";
        if (osName.toUpperCase().contains("WINDOWS"))  //LINUX
            filePath = Objects.requireNonNull(HikariUtil.class.getClassLoader().getResource(mysql_target_file)).getFile();
        else
            filePath = "/opt/module/realtime/myDB/" + mysql_target_file;//此地址使用linux服务器上面的绝对地址，根据地址自行修改
        FileReader fileReader = new FileReader(filePath);
        properties = new Properties();
        properties.load(fileReader);
        properties.forEach((k, v) -> {
            log.debug(String.format("key:%s value:%S", k, v));
            System.out.println("properties=============" + String.format("key:%s value:%S", k, v));
        });
        log.info("初始化配置文件成功.....");
        System.out.println("初始化配置文件成功.....");
    }

    /**
     * 注册HikariCP
     */
    private void registerHikariCP() {
        if (null != dataSource) {
            return;
        }
        HikariConfig config = new HikariConfig(properties);
        dataSource = new HikariDataSource(config);
    }

    /**
     * 提供对外 获取 HikariCPDatasource 的方法
     *
     * @return
     */
    public static DataSource getHikariCPDataSource() {
        if (null != dataSource) {
            return dataSource;
        }
        try {
            HIKARI_CP_UTI.initConfig();
            HIKARI_CP_UTI.registerHikariCP();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataSource;
    }

    /**
     * 提供对外链接
     */
    public static Connection getconn() {
        try {
            if (conn == null) {
                if (dataSource != null) {
                    conn = dataSource.getConnection();
                } else {
                    conn = getHikariCPDataSource().getConnection();
                }
            }
            //设置手动提交
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * 提交事务
     */
    public static void commit() {
        if (conn != null) {
            try {
                conn.commit();
            } catch (SQLException e) {
                log.error("提交事务失败,Connection:" + conn);
                System.out.println("提交事务失败,Connection:" + conn);
                e.printStackTrace();
            }
        }
    }

    /**
     * 事务回滚
     */
    public static void rollback() {
        if (conn != null) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                log.error("事务回滚失败,Connection:" + conn);
                System.out.println("事务回滚失败,Connection:" + conn);
                e.printStackTrace();
            }
        }
    }

    /**
     * 关闭连接
     */
    public static void close() {
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                log.error("关闭连接失败,Connection:" + conn);
                System.out.println("关闭连接失败,Connection:" + conn);
                e.printStackTrace();
            }
        }
    }

}

