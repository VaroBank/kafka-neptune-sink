package com.varobank.common.gremlin.utils;

import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.ser.Serializers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import software.amazon.neptune.cluster.ClusterEndpointsRefreshAgent;
import software.amazon.neptune.cluster.EndpointsType;

import java.util.Collection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Configuration
public class ConnectionCluster {

    private static final Logger logger = LoggerFactory.getLogger(ConnectionCluster.class);

    private final ScheduledExecutorService scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();

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
        scheduledExecutorService.scheduleWithFixedDelay(refreshEndpoints, delay, delay, timeUnit);
    }

    public Client connect() {
        Client client = cluster.connect();
        startPollingNeptuneAPI(60, TimeUnit.SECONDS);
        return client;
    }

    public void close() throws Exception {
        if (cluster != null) {
            cluster.close();
        }
    }
}
