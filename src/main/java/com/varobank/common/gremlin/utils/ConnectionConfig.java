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

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.remote.RemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

import java.io.PrintWriter;
import java.io.StringWriter;

@Configuration
public class ConnectionConfig implements RetryCondition {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionConfig.class);

    @Autowired
    private ConnectionCluster cluster;

    private GraphTraversalSource traversalSource;

    public void setCluster(ConnectionCluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Connects to Gremlin cluster and returns the graph traversal source
     * @return GraphTraversalSource
     */
    public GraphTraversalSource traversalSource() {
        if (!cluster.isCreated()) {
            traversalSource = null;
            cluster.createCluster();
        }

        if (traversalSource == null) {
            RemoteConnection connection = createRemoteConnection();
            traversalSource = AnonymousTraversalSource.traversal().withRemote(connection);
        }

        return traversalSource;
    }

    public RemoteConnection createRemoteConnection() {
        Client client = cluster.connect();
        return DriverRemoteConnection.using(client);
    }

    public void close() throws Exception {
        cluster.close();
    }

    public String getNeptuneEndpoint() {
        return cluster.getNeptuneEndpoint();
    }

    /**
     * Determines if the given exception is retriable or not
     * @param e
     * @return true if retriable else false
     */
    @Override
    public boolean allowRetry(Throwable e) {
        StringWriter stringWriter = new StringWriter();
        e.printStackTrace(new PrintWriter(stringWriter));
        String message = stringWriter.toString();

        logger.warn(String.format("Determining whether this is a retriable Cluster error: %s", message));

        if (message.contains("Timed out while waiting for an available host") ||
                message.contains("Timed-out waiting for connection on Host") ||
                message.contains("Connection to server is no longer active") ||
                message.contains("Connection reset by peer") ||
                message.contains("SSLEngine closed already") ||
                message.contains("Pool is shutdown") ||
                message.contains("ExtendedClosedChannelException") ||
                message.contains("Broken pipe") ||
                message.contains("NoHostAvailableException") ||
                getNeptuneEndpoint() != null && message.contains(getNeptuneEndpoint()) ||
                !cluster.isReadCluster() && message.contains("ReadOnlyViolationException")) {
            try {
                if (cluster != null && cluster.isCreated()) {
                    close();
                    logger.warn("Closed connection to the cluster");
                }
            } catch (Exception ex) {
                logger.error("Error closing cluster", ex);
            }
            cluster.setCluster(null);
            return true;
        }
        return false;
    }
}
