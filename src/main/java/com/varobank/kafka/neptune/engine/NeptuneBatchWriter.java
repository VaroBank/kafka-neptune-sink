package com.varobank.kafka.neptune.engine;

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

    private final RetryClient retryClient = new RetryClient();

    @Value("${cluster.id}")
    private String clusterId;

    @Autowired
    private ConnectionConfig connectionConfig;

    @Autowired
    private NeptuneSchema rawSchema;

    public void setConnectionConfig(ConnectionConfig connectionConfig) {
        this.connectionConfig = connectionConfig;
    }

    public void setClusterId(String clusterId) {
        this.clusterId = clusterId;
    }

    public BatchWriteQueries createBatchWriteQueries() {
        return new BatchWriteQueries(connectionConfig, rawSchema);
    }

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

    private void addVerticesBatchQuery(ConsumerRecord<String, String> record, String topic, long batchTime, BatchWriteQueries query) {
        JSONObject json = new JSONObject(record.value());
        if (json != null && json.has("payload")) {
            JSONObject payload = json.getJSONObject("payload");
            if (payload != null && payload.has("op")) {
                long ts_ms = payload.has("ts_ms") ? payload.getLong("ts_ms") : 0;
                long insertDateTime = batchTime;
                String op = payload.getString("op");
                if (op.equals("d")) {
                    if (payload.has("before")) {
                        JSONObject before = payload.getJSONObject("before");
                        query.deleteVertex(before, topic, op, ts_ms, insertDateTime);
                    }
                } else {
                    if (payload.has("after")) {
                        JSONObject after = payload.getJSONObject("after");
                        query.upsertVertex(after, topic, op, ts_ms, insertDateTime);
                    }
                }
            }
        }
    }


    private void addEdgesBatchQuery(ConsumerRecord<String, String> record, String topic, long batchTime, BatchWriteQueries query) {
        Map<String, String> settings = query.getSchema().get(topic);
        if (record != null && (settings.containsKey("child") || query.isCreatableParentRelationship(topic))) {
            JSONObject json = new JSONObject(record.value());
            if (json != null && json.has("payload")) {
                JSONObject payload = json.getJSONObject("payload");
                if (payload != null && payload.has("op")) {
                    long ts_ms = payload.has("ts_ms") ? payload.getLong("ts_ms") : 0;
                    long insertDateTime = batchTime;
                    String op = payload.getString("op");
                    if (op.equals("d")) {
                    } else {
                        if (payload.has("after")) {
                            JSONObject after = payload.getJSONObject("after");
                            query.upsertEdge(after, topic, op, ts_ms, insertDateTime);
                        }
                    }
                }
            }
        }
    }
}
