package yhh.com.mask.query;

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.SchemaPlus;
import org.springframework.stereotype.Component;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import static yhh.com.mask.common.Constants.CALCITE_AUTH_PASSWD;
import static yhh.com.mask.common.Constants.CALCITE_AUTH_USER;

@Component
public class QueryConnection {
    //calcite 连接多数据源
    public static Connection getConnectionDatabase(String modelPath) throws Exception {
        // 创建calcite的连接对象connection以及rootSchema
        // the properties for calcite connection
        Properties info = new Properties();
        info.setProperty("lex", "MYSQL");
        info.put("model", modelPath);

        String sql =
                "SELECT * FROM dept";
        Connection conn = null;
        try {
            conn = DriverManager.getConnection("jdbc:calcite:", info);
            try (Statement stmt = conn.createStatement()) {
                try (ResultSet rs = stmt.executeQuery(sql)) {
                    while (rs.next()) {
                        System.out.println(rs.getObject(1) + "__" + rs.getObject(2));
                    }
                }
            }
        } finally {
            closeQuietly(conn);
        }
        return conn;
    }


    public static Connection getConnection(String modelPath) throws Exception {
        Properties info = new Properties();
        info.setProperty("lex", "JAVA");
        info.put("model", modelPath);
        info.put("user", CALCITE_AUTH_USER);
        info.put("password", CALCITE_AUTH_PASSWD);
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
                System.out.println("calcite断开数据库");
                closeable.close();
            }
        } catch (final Exception ioe) {
            ioe.printStackTrace();
        }
    }
}
