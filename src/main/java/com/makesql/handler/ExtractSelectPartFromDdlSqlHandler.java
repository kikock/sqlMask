package com.makesql.handler;

import com.makesql.common.MaskException;
import com.makesql.query.QueryUtil;
import org.apache.calcite.mask.MaskContext;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.ddl.SqlCreateTable;
import org.apache.calcite.sql.ddl.SqlCreateView;
import org.apache.calcite.sql.parser.SqlParseException;

import java.util.List;

public class ExtractSelectPartFromDdlSqlHandler implements Handler {

    private final MaskContext context;

    public ExtractSelectPartFromDdlSqlHandler(MaskContext context) {
        this.context = context;
    }

    @Override
    public String processSql(String sql) {
        return getSelectPartFromDdlSql(sql);
    }

    private String getSelectPartFromDdlSql(String sql) {
        SqlNode sqlNode = null;
        try {
            sqlNode = QueryUtil.getSqlNode(sql);
        } catch (SqlParseException e) {
            e.printStackTrace();
        }
        SqlNode queryNode = getQuerySql(sqlNode);
        int startColumn = queryNode.getParserPosition().getColumnNum();
        context.setDdlPrefix(sql.substring(0, startColumn - 1));
        return sql.substring(startColumn - 1);
    }

    private SqlNode getQuerySql(SqlNode node) {
        for (; ; ) {
            if (node instanceof SqlSelect || node instanceof SqlWith) {
                return node;
            } else if (node instanceof SqlOrderBy) {
                node = ((SqlOrderBy) node).query;
            } else if (node instanceof SqlCreateTable) {
                List<SqlNode> nodes = ((SqlCreateTable) node).getOperandList();
                node = nodes.get(nodes.size() - 1);
            } else if (node instanceof SqlCreateView) {
                List<SqlNode> nodes = ((SqlCreateView) node).getOperandList();
                node = nodes.get(nodes.size() - 1);
            } else if (node instanceof SqlInsert) {
                node = ((SqlInsert) node).getSource();
            } else {
                throw new MaskException("other situation");
            }
        }
    }
}
