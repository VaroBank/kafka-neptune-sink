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
