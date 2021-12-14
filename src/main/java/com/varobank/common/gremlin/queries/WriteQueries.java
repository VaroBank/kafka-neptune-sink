package com.varobank.common.gremlin.queries;

import com.varobank.common.gremlin.utils.ConnectionConfig;
import com.varobank.common.gremlin.utils.NeptuneSchema;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.WithOptions;
import org.apache.tinkerpop.gremlin.structure.T;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.addV;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;


public class WriteQueries extends BaseQueries {

    private GraphTraversalSource traversalSource;

    private static final Logger logger = LoggerFactory.getLogger(WriteQueries.class);

    public WriteQueries() {
    }

    public WriteQueries(ConnectionConfig connectionConfig, NeptuneSchema rawSchema) {
        this.traversalSource = connectionConfig.traversalSource();
        setRawSchema(rawSchema);
    }

    public void clear() {
        long countToDelete = traversalSource.V().count().toList().get(0);

        int limit = 1000;
        while (countToDelete > 0) {
            traversalSource.V().limit(limit).drop().iterate();
            countToDelete -= limit;
        }
    }

    public void clearNeptuneVertices(String rootVertex) {
        Map<String, String> settings = getSchema().get(rootVertex);
        clearByVertex(rootVertex);
        if (settings.containsKey("child")) {
            String childVerticesStr = settings.get("child");
            String[] childVertices = childVerticesStr.split(",");
            for (String childVertex : childVertices) {
                clearByVertex(childVertex);
                clearNeptuneVertices(childVertex);
            }
        }
    }

    private void clearByVertex(String vertex) {
        long countToDelete = traversalSource.V().hasLabel(vertex).count().toList().get(0);
        logger.info("Deleting " + countToDelete + " records from " + vertex);

        int limit = 1000;
        while (countToDelete > 0) {
            traversalSource.V().hasLabel(vertex).limit(limit).drop().iterate();
            countToDelete -= limit;
        }
    }
}
