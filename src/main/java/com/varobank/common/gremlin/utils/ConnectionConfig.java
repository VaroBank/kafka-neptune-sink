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
