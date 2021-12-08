/*
Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.

Permission is hereby granted, free of charge, to any person obtaining a copy of this
software and associated documentation files (the "Software"), to deal in the Software
without restriction, including without limitation the rights to use, copy, modify,
merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
permit persons to whom the Software is furnished to do so.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*/

package com.varobank.common.gremlin.queries;

import com.varobank.common.gremlin.utils.ActivityTimer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;

public class GremlinTraversal {

    private GraphTraversal<?, ?> traversal;
    private GraphTraversalSource traversalSource;

    protected GremlinTraversal(GraphTraversalSource traversalSource) {
        this.traversalSource = traversalSource;
    }

    protected GremlinTraversal(GraphTraversal<?, ?> traversal) {
        this.traversal = traversal;
    }

    protected GraphTraversal<?, ?> V(final Object... vertexIds) {
        if (traversal == null) {
            return traversalSource.V(vertexIds);
        } else {
            return traversal.V(vertexIds);
        }
    }

    protected GraphTraversal<?, ?> addV(final String label) {
        if (traversal == null) {
            return traversalSource.addV(label);
        } else {
            return traversal.addV(label);
        }
    }

    protected long execute(long batchId) {
        ActivityTimer timer = new ActivityTimer("Execute query [" + batchId + "]");
        if (traversal != null) {
            traversal.forEachRemaining(e -> {
            });
        }
        return timer.calculateDuration(true);
    }
}
