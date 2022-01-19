/*
Copyright (c) 2022 Varo Bank, N.A. All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

1. Redistributions of source code must retain the above copyright notice, this
   list of conditions and the following disclaimer.

2. Redistributions in binary form must reproduce the above copyright notice,
   this list of conditions and the following disclaimer in the documentation
   and/or other materials provided with the distribution.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/
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

    public void validateSchema(JSONObject json, String topic) throws InvalidSchemaException {
        try {
            Map<String, String> settings = getSchema().get(topic);
            String id = settings.get("id");
            String prefix = settings.get("prefix");
            String idValue = json.get(id).toString();
            if (idValue.isEmpty()) {
                throw new InvalidSchemaException("Invalid Neptune schema. Empty id value: " + id + " for topic: " + topic);
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
            throw new InvalidSchemaException("Invalid Neptune schema " + e.getMessage(), e);
        }
    }
}
