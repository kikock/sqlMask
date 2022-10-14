package com.makesql;


import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.calcite.adapter.jdbc.JdbcSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.mask.MaskContext;
import org.apache.calcite.mask.MaskContextFacade;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

@SpringBootTest
public class CalciteTest {

    // calcite  直接连接数据库查询语句
    @Test
    public void sqlTest1() throws Exception {
        // 驱动
        // check driver exist
        Class.forName("org.apache.calcite.jdbc.Driver");
        Class.forName("com.mysql.jdbc.Driver");
        // 创建calcite的连接对象connection以及rootSchema
        // the properties for calcite connection
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        info.setProperty("remarks", "true");
        // SqlParserImpl can analysis sql dialect for sql parse
        info.setProperty("parserFactory", "org.apache.calcite.sql.parser.impl.SqlParserImpl#FACTORY");

        // create calcite connection and schema
        Connection conn = DriverManager.getConnection("jdbc:calcite:", info);
        CalciteConnection calciteConnection = conn.unwrap(CalciteConnection.class);
        System.out.println(calciteConnection.getProperties());
        SchemaPlus rootSchema = calciteConnection.getRootSchema();
        // 创建mysql的datasource

        // code for mysql datasource
        MysqlDataSource dataSource = new MysqlDataSource();
        // please change host and port maybe like "jdbc:mysql://127.0.0.1:3306/test"
        dataSource.setUrl("jdbc:mysql://127.0.0.1:3306/sales");
        dataSource.setUser("root");
        dataSource.setPassword("111111");
        // 创建mysql的schema

        // mysql schema, the sub schema for rootSchema, "test" is a schema in mysql
        Schema schema = JdbcSchema.create(rootSchema, "sales", dataSource, null, "sales");
        // 将mysql的schema添加到rootSchema中
        rootSchema.add("sales", schema);
        // run sql query
        Statement statement = calciteConnection.createStatement();
        // 执行SQL
        ResultSet resultSet = statement.executeQuery("select * from sales.emps");
        while (resultSet.next()) {
            // 打印SQL结果
            System.out.println(resultSet.getObject(1) + "__" + resultSet.getObject(2));
        }


    }

    // calcite  配置文件查询数据库
    @Test
    public void sqlTest2() throws Exception {
        MaskContextFacade.getCurrentContext(Thread.currentThread().getName());
        String path =
                this.getClass().getClassLoader().getResource("mysql-source.json").getPath();
        System.out.println(path);
        Properties config = new Properties();
        config.put("model", path);
        config.put("lex", "MYSQL");
        String sql = "select name , deptno from emps";

        Connection conn = null;
        Statement stmt = null;
        try {
            conn = DriverManager.getConnection("jdbc:calcite:", config);
            stmt = conn.createStatement();
            // 执行SQL
            ResultSet resultSet = stmt.executeQuery(sql);
            while (resultSet.next()) {
                // 打印SQL结果
                System.out.println(resultSet.getObject(1) + "__" + resultSet.getObject(2));
            }
        } finally {
            stmt.close();
            conn.close();
        }
    }


}
