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

import com.varobank.common.neptune.api.ClusterEndpointsRefreshAgent;
import com.varobank.common.neptune.api.EndpointsType;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.util.Collection;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Configuration
public class ConnectionCluster {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionCluster.class);

    private ClusterEndpointsRefreshAgent refreshAgent = null;
    private Cluster cluster = null;
    private Collection<String> endpoints = null;

    @Value("${neptune.cluster.id}")
    private String neptuneClusterId;

    @Value("${neptune.endpoint.type}")
    private String neptuneEndpointType;

    @Value("${neptune.port}")
    private String neptunePort;

    @Value("${neptune.ssl}")
    private String neptuneSsl;

    public boolean isCreated() {
        return cluster != null;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public String getNeptuneClusterId() {
        return neptuneClusterId;
    }

    public void setNeptuneClusterId(String neptuneClusterId) {
        this.neptuneClusterId = neptuneClusterId;
    }

    public void setNeptunePort(String neptunePort) {
        this.neptunePort = neptunePort;
    }

    public void setNeptuneSsl(String neptuneSsl) {
        this.neptuneSsl = neptuneSsl;
    }

    public void setNeptuneEndpointType(String neptuneEndpointType) {
        this.neptuneEndpointType = neptuneEndpointType;
    }

    public String getNeptuneEndpoint() {
        if (getSelector().equals(EndpointsType.Primary)) {
            try {
                Collection<String> endpoints = refreshAgent.getAddresses().get(EndpointsType.Primary);
                if (endpoints != null && !endpoints.isEmpty()) {
                    return endpoints.stream().findFirst().orElse(null);
                }
            } catch (Exception e) {
                logger.error("getNeptuneEndpoint " + e.getMessage(), e);
            }
        }
        return null;
    }

    public boolean isReadCluster() {
        return neptuneEndpointType != null && neptuneEndpointType.equals("read");
    }

    private EndpointsType getSelector() {
        return isReadCluster() ? EndpointsType.ReadReplicas : EndpointsType.Primary;
    }

    public Collection<String> getEndpoints() {
        refreshAgent = refreshAgent == null ? new ClusterEndpointsRefreshAgent(neptuneClusterId, getSelector()) : refreshAgent;
        return refreshAgent.getAddresses().get(getSelector());
    }

    private Cluster buildCluster(Collection<String> endpoints) {
        return Cluster.build()
                .addContactPoints(endpoints.toArray(new String[endpoints.size()]))
                .port(Integer.parseInt(neptunePort))
                .enableSsl(Boolean.parseBoolean(neptuneSsl))
                .minConnectionPoolSize(1)
                .maxConnectionPoolSize(5)
                .maxSimultaneousUsagePerConnection(16)
                .maxInProcessPerConnection(16)
                .serializer(Serializers.GRAPHBINARY_V1D0)
                .create();
    }

    public void createCluster() {
        logger.info("Creating cluster");
        endpoints = getEndpoints();
        this.cluster = buildCluster(endpoints);
        logger.info("Cluster created connected to the endpoints: " + endpoints.stream().collect(Collectors.joining(", ")));
    }

    public void startPollingNeptuneAPI(long delay, TimeUnit timeUnit) {
        Runnable refreshEndpoints = () -> {
            Collection<String> newEndpoints = getEndpoints();
            if (!newEndpoints.isEmpty()) {
                if (newEndpoints.size() == endpoints.size() && endpoints.containsAll(newEndpoints)) {
                    // Do nothing
                } else {
                    logger.info("Closing cluster with old endpoints: " + endpoints.stream().collect(Collectors.joining(", ")));
                    logger.info("New cluster endpoints found: " + newEndpoints.stream().collect(Collectors.joining(", ")));
                    cluster.close();
                    setCluster(null);
                }
            }
        };
        refreshAgent.startPollingNeptuneAPI(refreshEndpoints, delay, timeUnit);
    }

    public Client connect() {
        Client client = cluster.connect();
        startPollingNeptuneAPI(60, TimeUnit.SECONDS);
        return client;
    }

    public void close() throws Exception {
        try {
            if (refreshAgent != null) {
                refreshAgent.close();
                refreshAgent = null;
            }
        } finally {
            if (cluster != null) {
                cluster.close();
                cluster = null;
            }
        }
    }
}
