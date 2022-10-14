package com.makesql.service;


import com.makesql.handler.AddColumnAliasHandler;
import com.makesql.handler.ExecuteValidatorHandler;
import com.makesql.handler.ExpandStarAndCaseWhenHandler;
import com.makesql.handler.ExtractOriginColumnHandler;
import com.makesql.handler.ExtractSelectPartFromDdlSqlHandler;
import com.makesql.handler.HandlerChain;
import com.makesql.handler.RemoveSqlCommentHandler;
import com.makesql.handler.RewriteSqlWithPolicyHandler;
import com.makesql.query.QueryConnection;
import com.makesql.query.QueryUtil;
import lombok.extern.log4j.Log4j2;
import org.apache.calcite.mask.MaskContext;
import org.apache.calcite.mask.MaskContextFacade;
import org.apache.calcite.sql.parser.SqlParseException;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;
import sun.util.calendar.LocalGregorianCalendar;


import javax.annotation.PostConstruct;
import java.io.File;
import java.sql.Connection;
import java.sql.Statement;
import java.util.Locale;
@Service
public class QueryService {

    private Statement stmt;

    public String getMaskSql(String originSql) throws Exception {
        MaskContext context = MaskContextFacade.current();
        Thread.currentThread().setName(context.getMaskId());
        try {
            HandlerChain handlerChain = new HandlerChain();
            addHandler(context, handlerChain);
            String ret = handlerChain.handler(originSql);
            ret = QueryUtil.getSqlNode(ret).toSqlString(null, true)
                    .getSql().replace("`", "")
                    .toLowerCase(Locale.ROOT);
            return context.getDdlPrefix() + ret;
        } finally {
            MaskContextFacade.resetCurrent();
        }
    }
    private void addHandler(MaskContext context, HandlerChain handlerChain) throws Exception {
        String path =
                this.getClass().getClassLoader().getResource("mysql-source.json").getPath();
        File file = new File(path);
        Connection conn = QueryConnection.getConnectionMysql(file.getAbsolutePath());
        stmt = conn.createStatement();
        handlerChain.addHandler(new RemoveSqlCommentHandler());
        handlerChain.addHandler(new ExtractSelectPartFromDdlSqlHandler(context));
        handlerChain.addHandler(new ExpandStarAndCaseWhenHandler(context, stmt));
        handlerChain.addHandler(new ExecuteValidatorHandler(stmt));
        handlerChain.addHandler(new AddColumnAliasHandler(context));
        handlerChain.addHandler(new ExtractOriginColumnHandler(context));
        handlerChain.addHandler(new RewriteSqlWithPolicyHandler(context));
    }

}
