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
package com.varobank.kafka.neptune.engine;

import com.varobank.common.gremlin.queries.GremlinQueriesObjectFactory;
import com.varobank.common.gremlin.utils.*;
import com.varobank.kafka.neptune.utils.Metrics;
import com.varobank.kafka.neptune.utils.RetryClient;
import com.varobank.common.gremlin.queries.BatchWriteQueries;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
public class NeptuneBatchWriter {

    private final Logger logger = LoggerFactory.getLogger(NeptuneBatchWriter.class);

    private static final String OP = "op";
    private static final String DELETE = "d";
    private static final String AFTER = "after";
    private static final String BEFORE = "before";
    private static final String PAYLOAD = "payload";
    private static final String TS_MS = "ts_ms";


    private final RetryClient retryClient = new RetryClient();

    @Value("${cluster.id}")
    private String clusterId;

    @Autowired
    private ConnectionConfig connectionConfig;

    @Autowired
    private GremlinQueriesObjectFactory queriesObjectFactory;

    @Autowired
    private Schema schema;

    public void setConnectionConfig(ConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public BatchWriteQueries createBatchWriteQueries() {
        return queriesObjectFactory.createBatchWriteQueries();
    }

    /**
     * Validates the user defined Neptune schema against the kafka messages - if not valid throws an Exception
     * @param records
     * @throws InvalidSchemaException
     */
    public void validateSchema(List<ConsumerRecord<String, String>> records) throws InvalidSchemaException {
        for (ConsumerRecord<String, String> record : records) {
            if (record != null) {
                String topic = record.topic();
                JSONObject payload = getPayloadJson(record.value());
                if (payload != null) {
                    JSONObject json = payload.getString(OP).equals(DELETE) && payload.has(BEFORE) ? payload.getJSONObject(BEFORE) :
                            (payload.has(AFTER) ? payload.getJSONObject(AFTER) : null);
                    if (json != null) {
                        schema.validateSchema(json, topic);
                    } else {
                        logger.warn("Ignoring message with no " + BEFORE + "/" + AFTER + ", " + record.value());
                    }
                }
            } else {
                logger.warn("Ignoring null kafka message");
            }
        }
    }

    /**
     * Writes the batch of kafka JSON massages to Neptune using BatchWriteQueries methods to upsert vertices and edges.
     * If the batch write query fails and the exception is retriable then the query will be retried for up to 5 times.
     * @param records
     * @throws Exception
     */
    public void writeToNeptune(List<ConsumerRecord<String, String>> records) throws Exception {
        Metrics metrics = new Metrics(clusterId);
        long batchTime = System.currentTimeMillis();

        try (ActivityTimer batchTimer = new ActivityTimer("TOTAL write batch: " + batchTime)) {

            Runnable retriableQuery = () -> {

                BatchWriteQueries query = createBatchWriteQueries();

                try (ActivityTimer timer = new ActivityTimer("Parse batch: " + batchTime)) {
                    records.forEach(record -> {
                        if (record != null) {
                            addVerticesBatchQuery(record, record.topic(), batchTime, query);
                        }
                    });

                    records.forEach(record -> {
                        addEdgesBatchQuery(record, record.topic(), batchTime, query);
                    });
                }

                query.execute(batchTime);
            };

            int retryCount = retryClient.retry(
                    retriableQuery,
                    5,
                    RetryCondition.containsMessage("ConcurrentModificationException"),
                    RetryCondition.containsMessage("ConstraintViolationException"),
                    connectionConfig);

            metrics.add(records.size(), batchTimer.calculateDuration(false), retryCount);
        }
        metrics.publish();
    }

    private String cleanUpTopic(String topic) {
        // Clean retry topic
        if (topic.startsWith("retry.")) {
            topic = topic.replaceFirst("retry.", "");
        }
        if (topic.matches(".*-retry-\\d-\\d$")) {
            topic = topic.replaceAll("-retry-\\d-\\d$", "");
        }
        if (topic.matches(".*-retry-\\d$")) {
            topic = topic.replaceAll("-retry-\\d$", "");
        }
        if (topic.endsWith("__retry")) {
            topic = topic.split("__")[0];
        }

        return topic;
    }

    /**
     * Writes a single kafka JSON massages to Neptune using BatchWriteQueries methods to upsert vertices and edges.
     * If the batch write query fails and the exception is retriable then the query will be retried for up to 5 times.
     * @param record
     * @throws Exception
     */
    public void retryWriteToNeptune(ConsumerRecord<String, String> record) throws Exception {
        Metrics metrics = new Metrics(clusterId);
        long batchTime = System.currentTimeMillis();

        try (ActivityTimer batchTimer = new ActivityTimer("TOTAL retry batch: " + batchTime)) {
            Runnable retriableQuery = () -> {

                // Clean retry topic
                String topic = cleanUpTopic(record.topic());

                BatchWriteQueries query = createBatchWriteQueries();
                addVerticesBatchQuery(record, topic, batchTime, query);
                addEdgesBatchQuery(record, topic, batchTime, query);

                query.execute(batchTime);
            };

            int retryCount = retryClient.retry(
                    retriableQuery,
                    5,
                    RetryCondition.containsMessage("ConcurrentModificationException"),
                    RetryCondition.containsMessage("ConstraintViolationException"),
                    connectionConfig);

            metrics.add(1, batchTimer.calculateDuration(false), retryCount);
        }
        metrics.publish();
    }

    private JSONObject getPayloadJson(String message) {
        if (message != null) {
            JSONObject json = new JSONObject(message);
            if (json != null && json.has(PAYLOAD)) {
                JSONObject payload = json.getJSONObject(PAYLOAD);
                if (payload != null && payload.has(OP)) {
                    return payload;
                } else {
                    logger.warn("Ignoring message with no " + OP + ", " + message);
                }
            } else {
                logger.warn("Ignoring message with no " + PAYLOAD + ", " + message);
            }
        }

        return null;
    }

    private void addVerticesBatchQuery(ConsumerRecord<String, String> record, String topic, long batchTime, BatchWriteQueries query) {
        JSONObject payload = getPayloadJson(record.value());
        if (payload != null) {
            long ts_ms = payload.has(TS_MS) ? payload.getLong(TS_MS) : 0;
            long insertDateTime = batchTime;
            String op = payload.getString(OP);
            if (op.equals(DELETE)) {
                if (payload.has(BEFORE)) {
                    JSONObject before = payload.getJSONObject(BEFORE);
                    query.deleteVertex(before, topic, op, ts_ms, insertDateTime);
                }
            } else {
                if (payload.has(AFTER)) {
                    JSONObject after = payload.getJSONObject(AFTER);
                    query.upsertVertex(after, topic, op, ts_ms, insertDateTime);
                }
            }
        }
    }

    private void addEdgesBatchQuery(ConsumerRecord<String, String> record, String topic, long batchTime, BatchWriteQueries query) {
        Map<String, String> settings = query.getSchema().get(topic);
        if (record != null && (settings.containsKey("child") || query.isCreatableParentRelationship(topic))) {
            JSONObject payload = getPayloadJson(record.value());
            if (payload != null) {
                long ts_ms = payload.has(TS_MS) ? payload.getLong(TS_MS) : 0;
                long insertDateTime = batchTime;
                String op = payload.getString(OP);
                if (op.equals(DELETE)) {
                } else {
                    if (payload.has(AFTER)) {
                        JSONObject after = payload.getJSONObject(AFTER);
                        query.upsertEdge(after, topic, op, ts_ms, insertDateTime);
                    }
                }
            }
        }
    }
}
