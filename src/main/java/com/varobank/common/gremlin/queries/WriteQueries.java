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
package com.varobank.common.gremlin.queries;

import com.varobank.common.gremlin.utils.ConnectionConfig;
import com.varobank.common.gremlin.utils.NeptuneSchema;
import com.varobank.common.gremlin.utils.Schema;
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

    protected WriteQueries(ConnectionConfig connectionConfig, Schema schema) {
        this.traversalSource = connectionConfig.traversalSource();
        setSchema(schema);
    }

    /**
     * Drops all vertices and edges in the Neptune DB
     */
    public void clear() {
        long countToDelete = traversalSource.V().count().toList().get(0);

        int limit = 1000;
        while (countToDelete > 0) {
            traversalSource.V().limit(limit).drop().iterate();
            countToDelete -= limit;
        }
    }

    /**
     * Drops all child vertices and edges including the provided rootVertex
     * @param rootVertex
     */
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
