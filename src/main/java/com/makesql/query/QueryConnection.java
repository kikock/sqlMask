package com.makesql.query;

import org.apache.calcite.mask.MaskContext;
import org.apache.calcite.mask.MaskContextFacade;

import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;


public class QueryConnection {


    public static Connection getConnectionMysql(String modelPath) throws Exception {
        Properties info = new Properties();
        info.setProperty("lex", "MYSQL");
        info.put("model", modelPath);
        Connection conn = null;
        Statement stmt = null;
        String sql = "select * from  sales.emps";
        try {
            conn = DriverManager.getConnection("jdbc:calcite:", info);
        } finally {
            // closeQuietly(conn);
        }
        return conn;
    }

    public static Connection getConnection(String modelPath) throws Exception {
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        info.put("model", modelPath);
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:calcite:", info);
        } finally {
            closeQuietly(conn);
        }
        return conn;
    }

    public static void closeQuietly(final AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (final Exception ioe) {
            ioe.printStackTrace();
        }
    }
}
