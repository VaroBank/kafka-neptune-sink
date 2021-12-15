package com.varobank.common.gremlin.queries;

import com.varobank.common.gremlin.utils.Schema;

import java.util.Map;

public class BaseQueries {

    private Schema schema;

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public Map<String, Map<String, String>> getSchema() {
        return schema.getSchema();
    }

    public String getRootCustomerVertex() {
        return schema.getRootCustomerVertex();
    }

}
