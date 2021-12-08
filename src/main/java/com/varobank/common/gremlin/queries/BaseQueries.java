package com.varobank.common.gremlin.queries;

import com.varobank.common.gremlin.utils.NeptuneSchema;

import java.util.HashMap;
import java.util.Map;

public class BaseQueries {

    private NeptuneSchema rawSchema;

    private Map<String, Map<String, String>> schema;

    public void setRawSchema(NeptuneSchema rawSchema) {
        this.rawSchema = rawSchema;
    }

    public void setSchema(Map<String, Map<String, String>> schema) {
        this.schema = schema;
    }

    public Map<String, Map<String, String>> getSchema() {
        if (schema == null) {
            setDefaultSchema();
        }
        return schema;
    }

    public String getRootCustomerVertex() {
        return rawSchema.getRootCustomerVertex();
    }

    public void setRootCustomerVertex(String rootCustomerVertex) {
        rawSchema.setRootCustomerVertex(rootCustomerVertex);
    }

    public void setDefaultSchema() {
        Map<String, Map<String, String>> schema = new HashMap<>();
        for (NeptuneSchema.Schema rawSettings: rawSchema.getSchema()) {
            rawSchema.transformSchema(rawSettings, schema, null);
        }
        this.schema = schema;
    }
}
