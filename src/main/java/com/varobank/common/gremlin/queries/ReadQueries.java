package com.varobank.common.gremlin.queries;

import com.varobank.common.gremlin.utils.ConnectionConfig;
import com.varobank.common.gremlin.utils.NeptuneSchema;
import com.varobank.common.gremlin.utils.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;

public class ReadQueries extends BaseQueries {

    private ConnectionConfig connectionConfig;

    private static final Logger logger = LoggerFactory.getLogger(ReadQueries.class);

    public ReadQueries() {
    }

    protected ReadQueries(ConnectionConfig connectionConfig, Schema schema) {
        this.connectionConfig = connectionConfig;
        setSchema(schema);
    }

    /**
     * Get the vertex record by vertex label and vertex id.
     * @param topic
     * @param id
     * @return vertex record
     */
    public Map getById(String topic, String id) {
        List<Map<Object, Object>> resultSet = connectionConfig.traversalSource().V().hasLabel(topic).has(T.id, id).valueMap().by(unfold()).with(WithOptions.tokens).toList();
        return !resultSet.isEmpty() ? resultSet.get(0) : null;
    }

    /**
     * Get the vertex record by vertex id.
     * @param vertexId
     * @return vertex record
     */
    public Map getVertexById(String vertexId) {
        List<Map<Object, Object>> resultSet = connectionConfig.traversalSource().V(vertexId).valueMap().by(unfold()).toList();
        return !resultSet.isEmpty() ? resultSet.get(0) : null;
    }

    /**
     * Counts records for each vertex label not including the empty vertex records (empty vertex records are created if one
     * of the ends is missing when creating an edge between two vertices)
     * @return map of record counts by vertex label
     */
    public Map<Object, Long> countAll() {
        Map<Object, Long> verticesCount = connectionConfig.traversalSource().V().or(__.hasNot("ts_ms"), __.has("ts_ms", P.gt(0))).label().groupCount().toList().get(0);

        return verticesCount;
    }

    /**
     * Counts all records for each vertex label.
     * @return map of record counts by vertex label
     */
    public Map<Object, Long> countAllUnfiltered() {
        Map<Object, Long> verticesCount = connectionConfig.traversalSource().V().label().groupCount().toList().get(0);

        return verticesCount;
    }

    /**
     * Counts records by vertex label
     * @param vertex
     * @return number of records
     */
    public Long countByVertex(String vertex) {
        List<Long> countList = connectionConfig.traversalSource().V().hasLabel(vertex).count().toList();
        return !countList.isEmpty() ? countList.get(0) : null;
    }

    /**
     * Traverses all connected by edges vertex records starting from the provided root vertex id
     * @param customerId
     * @return map of vertex records by vertex label
     */
    public Map getCustomerDataFromAllVertices(String customerId) {
        Map<String, Map> result = new HashMap<>();
        Map<String, String> settings = getSchema().get(getRootCustomerVertex());
        String customerVertexId = settings.get("prefix") + customerId;
        populateVertexResultMap(getRootCustomerVertex(), customerVertexId, result);

        return result;
    }

    private void populateVertexResultMap(String vertexLabel, String vertexId, Map<String, Map> result) {
        Map map = getVertexById(vertexId);
        result.put(vertexLabel, map);

        List<Map<Object, Object>> resultSet = connectionConfig.traversalSource().V(vertexId).outE().inV().valueMap().by(unfold()).with(WithOptions.tokens, WithOptions.all).toList();
        for (Map<Object, Object> resultMap : resultSet) {
            String id = resultMap.get(T.id).toString();
            String label = resultMap.get(T.label).toString();

            populateVertexResultMap(label, id, result);
        }
    }
}
