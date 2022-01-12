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

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ConfigurationProperties(prefix = "neptune.settings")
@Component
public class NeptuneSchema {

    private final List<Schema> schema = new ArrayList<>();

    @Value("${neptune.settings.root_customer_vertex}")
    private String rootCustomerVertex;

    public String getRootCustomerVertex() {
        return rootCustomerVertex;
    }

    public void setRootCustomerVertex(String rootCustomerVertex) {
        this.rootCustomerVertex = rootCustomerVertex;
    }

    public List<Schema> getSchema() {
        return this.schema;
    }

    public static class Schema {

        private String vertex;
        private String id;
        private String refKey;
        private String prefix;
        private String edgeLabel;
        private String parentPropKey;
        private String kafkaTopic;
        private List<Schema> child;

        public String getVertex() {
            return vertex;
        }

        public String getId() {
            return id;
        }

        public String getRefKey() {
            return refKey;
        }

        public String getPrefix() {
            return prefix;
        }

        public String getEdgeLabel() {
            return edgeLabel;
        }

        public String getParentPropKey() {
            return parentPropKey;
        }

        public String getKafkaTopic() {
            return kafkaTopic;
        }

        public List<Schema> getChild() {
            return child;
        }

        public void setVertex(String vertex) {
            this.vertex = vertex;
        }

        public void setId(String id) {
            this.id = id;
        }

        public void setRefKey(String refKey) {
            this.refKey = refKey;
        }

        public void setPrefix(String prefix) {
            this.prefix = prefix;
        }

        public void setEdgeLabel(String edgeLabel) {
            this.edgeLabel = edgeLabel;
        }

        public void setParentPropKey(String parentPropKey) {
            this.parentPropKey = parentPropKey;
        }

        public void setKafkaTopic(String kafkaTopic) {
            this.kafkaTopic = kafkaTopic;
        }

        public void setChild(List<Schema> child) {
            this.child = child;
        }

    }

    /**
     * Transforms the Neptune schema defined via yaml into the kay-value map. The key-value map schema is used across the
     * read/write graph traversal queries.
     * @param rawSettings
     * @param schema
     * @param parent
     */
    public void transformSchema(NeptuneSchema.Schema rawSettings, Map<String, Map<String, String>> schema, String parent) {
        String vertex = rawSettings.getVertex();
        List<String> childVertices = new ArrayList<>();
        if (rawSettings.getChild() != null && !rawSettings.getChild().isEmpty()) {
            for (NeptuneSchema.Schema childSettings : rawSettings.getChild()) {
                transformSchema(childSettings, schema, vertex);
                childVertices.add(childSettings.getVertex());
            }
        }
        Map<String, String> settings = new HashMap<>();
        settings.put("prefix", rawSettings.getPrefix());
        settings.put("id", rawSettings.getId());
        settings.put("ref_key", rawSettings.getRefKey());
        settings.put("parent_prop_key", rawSettings.getParentPropKey());
        settings.put("edge_label", rawSettings.getEdgeLabel());
        if (rawSettings.getKafkaTopic() != null && !rawSettings.getKafkaTopic().isEmpty()) {
            settings.put("kafka_topic", rawSettings.getKafkaTopic());
        }
        if (!childVertices.isEmpty()) {
            settings.put("child", StringUtils.join(childVertices, ","));
        }
        if (parent != null) {
            settings.put("parent", parent);
        }

        schema.put(vertex, settings);
    }
}
