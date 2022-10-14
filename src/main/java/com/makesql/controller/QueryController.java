package com.makesql.controller;

import com.makesql.bean.SqlRequest;
import com.makesql.service.QueryService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;

@Controller
public class QueryController {
    @Autowired
    private QueryService queryService;

    @PostMapping(path = "/query")
    @ResponseBody
    public String query(@RequestBody SqlRequest sql) throws Exception {
        return queryService.getMaskSql(sql.getSql());
    }
}
