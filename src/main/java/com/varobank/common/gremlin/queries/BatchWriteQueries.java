package com.varobank.common.gremlin.queries;

import com.varobank.common.gremlin.utils.ConnectionConfig;
import com.varobank.common.gremlin.utils.NeptuneSchema;
import com.varobank.common.gremlin.utils.Schema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;

public class BatchWriteQueries extends BaseQueries {

    private GremlinTraversal traversal;
    private String emptyJSON = "{}";

    private Set<String> createdVerticesInBatch = new HashSet<>();

    private static final Logger logger = LoggerFactory.getLogger(BatchWriteQueries.class);

    public BatchWriteQueries() {
    }

    protected BatchWriteQueries(ConnectionConfig connectionConfig, Schema schema) {
        this.traversal = new GremlinTraversal(connectionConfig.traversalSource());
        setSchema(schema);
    }

    /**
     * Soft delete of the vertex record in Neptune DB. Neptune schema must be defined.
     * @param json
     * @param topic
     * @param op
     * @param ts_ms
     * @param insertDateTime
     */
    public void deleteVertex(JSONObject json, String topic, String op, long ts_ms, long insertDateTime) {
        Map<String, String> settings = getSchema().get(topic);
        String id = settings.get("id");
        String prefix = settings.get("prefix");
        String idValue = prefix + json.get(id);

        GraphTraversal g = traversal.V().hasLabel(topic).has(T.id, idValue).
                fold().
                coalesce(unfold(), addV(topic).property(T.id, idValue));

        traversal = new GremlinTraversal(g.
                property(Cardinality.single, "op", op).
                property(Cardinality.single, "ts_ms", ts_ms).
                property(Cardinality.single, "insertDateTime", insertDateTime));

    }

    /**
     * Creates or updates a vertex record in the Neptune DB from provided json. Neptune schema must be defined.
     * @param json
     * @param topic
     * @param op
     * @param ts_ms
     * @param insertDateTime
     */
    public void upsertVertex(JSONObject json, String topic, String op, long ts_ms, long insertDateTime) {
        Map<String, String> settings = getSchema().get(topic);
        if (settings == null) {
            logger.error("No schema defined for " + topic);
        }
        String id = settings.get("id");
        String prefix = settings.get("prefix");
        String idValue = prefix + json.get(id);

        GraphTraversal query = traversal.V(idValue).hasLabel(topic);
        GraphTraversal temp = query.asAdmin().clone().values("ts_ms");
        long prevTimestamp = temp.hasNext() ? ((Number)temp.next()).longValue() : 0L;
        if (ts_ms >= prevTimestamp) {
            GraphTraversal g = query.asAdmin().clone().
                    fold().
                    coalesce(unfold(),
                            addV(topic).property(T.id, idValue));
            json.keys().forEachRemaining(key -> {
                Object value = json.get(key);
                if (!key.equals("id") && !key.equals(id)) {
                    if (JSONObject.NULL == value) {
                        value = emptyJSON;
                    }
                    g.property(Cardinality.single, key, value);
                }
            });

            traversal = new GremlinTraversal(g.
                    property(Cardinality.single, "op", op).
                    property(Cardinality.single, "ts_ms", ts_ms).
                    property(Cardinality.single, "insertDateTime", insertDateTime));
        }
    }

    /**
     * Creates or updates an Edge record in the Neptune DB from provided json. Neptune schema must be defined.
     * In order to create an edge between two vertices those two vertices must exist. Prior to creating of an edge
     * the following conditions are checked:
     * - when creating an edge from parent vertex to child vertex (one-to-one or many-to-one relationship) child vertex
     *   must be pre-created if it doesn't exist (child vertex is pre-created as an empty vertex with just child vertex id)
     * - when creating an edge from child vertex to parent vertex (one-to-one or one-to-many relationship) parent vertex
     *   must be pre-created if it doesn't exist (as an empty vertex with the parent vertex id)
     * * All pre-created empty vertices are updated with the real data as soon as this data is pulled from the Kafka.
     * * Many-to-many relationship can only be created via an intermediate vertex using many-to-one and one-to-many
     *   relationships which has to defined in the Neptune schema.
     * @param json
     * @param topic
     * @param op
     * @param ts_ms
     * @param insertDateTime
     */
    public void upsertEdge(JSONObject json, String topic, String op, long ts_ms, long insertDateTime) {
        Map<String, String> settings = getSchema().get(topic);
        if (settings.containsKey("child")) {
            String id = settings.get("id");
            String prefix = settings.get("prefix");
            String fromVertexId = prefix + json.get(id);

            String childTopicsStr = settings.get("child");
            String[] childTopics = childTopicsStr.split(",");
            for (String childTopic: childTopics) {
                Map<String, String> childSettings = getSchema().get(childTopic);
                if (childSettings.containsKey("kafka_topic")) {
                    String connectingPropKey = childSettings.get("parent_prop_key");
                    String edgeLabel = childSettings.get("edge_label");
                    if (childSettings.get("id").equals(childSettings.get("ref_key"))) {
                        String childPrefix = childSettings.get("prefix");
                        String connectingPropKeyValue = json.get(connectingPropKey) != null ? json.get(connectingPropKey).toString() : null;
                        if (connectingPropKeyValue != null && !connectingPropKeyValue.isEmpty()) {
                            String toVertexId = childPrefix + connectingPropKeyValue;
                            // 1. Pre-create child vertx if enough info in current JSON message about the child
                            // This allows to create many to one and one to one relationship
                            createVertexIfNotExist(toVertexId, childTopic, insertDateTime);
                            addEdge(fromVertexId, toVertexId, edgeLabel, insertDateTime);
                        } else {
                            logger.warn("connectingPropKey " + connectingPropKey + " is empty. Topic: " + topic + " json: " + json);
                        }

                    }
                }
            }
        }
        if (isCreatableParentRelationship(topic)) {
            String parentPropKey = settings.get("parent_prop_key");
            String connectingRefKey = settings.get("ref_key");
            String parentTopic = settings.get("parent");
            Map<String, String> parentSettings = getSchema().get(parentTopic);
            String parentId = parentSettings.get("id");

            String prefix = settings.get("prefix");
            String id = settings.get("id");
            String toVertexId = prefix + json.get(id);
            String edgeLabel = settings.get("edge_label");
            if (parentId.equals(parentPropKey)) {
                // 2. Pre-create parent vertx if enough info in current JSON message about the parent
                // This allows to create one to many and one to one relationship
                String parentPrefix = parentSettings.get("prefix");
                String connectingRefKeyValue = json.get(connectingRefKey) != null ? json.get(connectingRefKey).toString() : null;
                if (connectingRefKeyValue != null && !connectingRefKeyValue.isEmpty()) {
                    String fromVertexId = parentPrefix + connectingRefKeyValue;
                    //case the parent vertex can be pre-created if not exists
                    createVertexIfNotExist(fromVertexId, parentTopic, insertDateTime);
                    addEdge(fromVertexId, toVertexId, edgeLabel, insertDateTime);
                } else {
                    logger.warn("connectingRefKey " + connectingRefKey + " is empty. Topic: " + topic + " json: " + json);
                }
            }
        }
    }

    void addEdge(String fromVertexId, String toVertexId, String edgeLabel, long insertDateTime) {

        String edgeId = String.format("%s-%s", fromVertexId, toVertexId);
        logger.info("Adding edge: " + edgeId);

        traversal = new GremlinTraversal(traversal.
                V(fromVertexId).outE(edgeLabel).hasId(edgeId).fold().coalesce(
                unfold(),
                V(fromVertexId).addE(edgeLabel).to(V(toVertexId)).
                        property(T.id, edgeId).
                        property(Cardinality.single,"insertDateTime", insertDateTime)));
    }

    public boolean isCreatableParentRelationship(String topic) {
        Map<String, String> settings = getSchema().get(topic);
        if (settings.containsKey("parent") && settings.containsKey("kafka_topic")) {
            String parentPropKey = settings.get("parent_prop_key");
            String parentTopic = settings.get("parent");
            Map<String, String> parentSettings = getSchema().get(parentTopic);
            String parentId = parentSettings.get("id");
            if (parentId.equals(parentPropKey)) {
                return true;
            }
        }

        return false;
    }

    void createVertexIfNotExist(String vertexId, String topic, long insertDateTime) {
        if (!createdVerticesInBatch.contains(vertexId)) {
            GraphTraversal query = traversal.V(vertexId).hasLabel(topic);
            GraphTraversal temp = query.asAdmin().clone().id();
            if (!temp.hasNext()) {
                GraphTraversal g = query.asAdmin().clone().
                        fold().
                        coalesce(unfold(),
                                addV(topic).property(T.id, vertexId)).
                                        property(Cardinality.single, "ts_ms", 0L).
                                        property(Cardinality.single, "insertDateTime", insertDateTime);
                traversal = new GremlinTraversal(g);
            }
            createdVerticesInBatch.add(vertexId);
        }
    }

    public long execute(long batchId) {
        if (traversal != null) {
            return traversal.execute(batchId);
        } else {
            return 0;
        }
    }
}
