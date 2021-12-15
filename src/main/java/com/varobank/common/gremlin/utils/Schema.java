package com.varobank.common.gremlin.utils;

import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class Schema {

    @Autowired
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

    public void validateSchema(JSONObject json, String topic) throws Exception {
        try {
            Map<String, String> settings = getSchema().get(topic);
            String id = settings.get("id");
            String prefix = settings.get("prefix");
            String idValue = json.get(id).toString();
            if (idValue.isEmpty()) {
                throw new Exception("Invalid Neptune schema. Empty id value: " + id + " for topic: " + topic);
            }
            if (settings.containsKey("child")) {
                String childTopicsStr = settings.get("child");
                String[] childTopics = childTopicsStr.split(",");
                for (String childTopic: childTopics) {
                    Map<String, String> childSettings = getSchema().get(childTopic);
                    if (childSettings.containsKey("kafka_topic")) {
                        String connectingPropKey = childSettings.get("parent_prop_key");
                        String edgeLabel = childSettings.get("edge_label");
                        if (childSettings.get("id").equals(childSettings.get("ref_key"))) {
                            String childPrefix = childSettings.get("prefix");
                            String toVertexId = json.get(connectingPropKey).toString();
                        }
                    }
                }
            }
            if (settings.containsKey("parent") && settings.containsKey("kafka_topic")) {
                String parentPropKey = settings.get("parent_prop_key");
                String connectingRefKey = settings.get("ref_key");
                String parentTopic = settings.get("parent");
                Map<String, String> parentSettings = getSchema().get(parentTopic);
                String parentId = parentSettings.get("id");
                if (parentId.equals(parentPropKey)) {
                    String parentPrefix = parentSettings.get("prefix");
                    String fromVertexId = json.get(connectingRefKey).toString();
                }
            }
        } catch (Exception e) {
            throw new Exception("Invalid Neptune schema " + e.getMessage(), e);
        }
    }
}
