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
package com.varobank.common.neptune.api;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.neptune.AmazonNeptune;
import com.amazonaws.services.neptune.AmazonNeptuneClientBuilder;
import com.amazonaws.services.neptune.model.*;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class GetEndpointsFromNeptuneManagementApi implements ClusterEndpointsFetchStrategy {

    private static final Logger logger = LoggerFactory.getLogger(GetEndpointsFromNeptuneManagementApi.class);

    private static final Map<String, Map<String, String>> instanceTags = new HashMap<>();

    public static final String DEFAULT_PROFILE = "default";

    private final String clusterId;
    private final String region;
    private final String iamProfile;
    private final AWSCredentialsProvider credentials;
    private final Collection<EndpointsSelector> selectors;
    private final AtomicReference<Map<EndpointsSelector, Collection<String>>> previousResults = new AtomicReference<>();

    public GetEndpointsFromNeptuneManagementApi(String clusterId, Collection<EndpointsSelector> selectors) {
        this(clusterId, selectors, RegionUtils.getCurrentRegionName());
    }

    public GetEndpointsFromNeptuneManagementApi(String clusterId,
                                                Collection<EndpointsSelector> selectors,
                                                String region) {
        this(clusterId, selectors, region, DEFAULT_PROFILE);
    }

    public GetEndpointsFromNeptuneManagementApi(String clusterId,
                                                Collection<EndpointsSelector> selectors,
                                                String region,
                                                String iamProfile) {
        this(clusterId, selectors, region, iamProfile, null);
    }

    public GetEndpointsFromNeptuneManagementApi(String clusterId,
                                                Collection<EndpointsSelector> selectors,
                                                String region,
                                                AWSCredentialsProvider credentials) {
        this(clusterId, selectors, region, DEFAULT_PROFILE, credentials);
    }

    private GetEndpointsFromNeptuneManagementApi(String clusterId,
                                                 Collection<EndpointsSelector> selectors,
                                                 String region,
                                                 String iamProfile,
                                                 AWSCredentialsProvider credentials) {
        this.clusterId = clusterId;
        this.selectors = selectors;
        this.region = region;
        this.iamProfile = iamProfile;
        this.credentials = credentials;
    }

    @Override
    public Map<EndpointsSelector, Collection<String>> getAddresses() {

        try {
            AmazonNeptuneClientBuilder builder = AmazonNeptuneClientBuilder.standard();

            if (StringUtils.isNotEmpty(region)){
                builder = builder.withRegion(region);
            }

            if (credentials != null){
                builder = builder.withCredentials(credentials);
            } else if (!iamProfile.equals(DEFAULT_PROFILE)){
                builder = builder.withCredentials(new ProfileCredentialsProvider(iamProfile));
            }

            AmazonNeptune neptune = builder.build();

            DescribeDBClustersResult describeDBClustersResult = neptune
                    .describeDBClusters(new DescribeDBClustersRequest().withDBClusterIdentifier(clusterId));

            if (describeDBClustersResult.getDBClusters().isEmpty()) {
                throw new IllegalStateException(String.format("Unable to find cluster %s", clusterId));
            }

            DBCluster dbCluster = describeDBClustersResult.getDBClusters().get(0);

            String clusterEndpoint = dbCluster.getEndpoint();
            String readerEndpoint = dbCluster.getReaderEndpoint();

            List<DBClusterMember> dbClusterMembers = dbCluster.getDBClusterMembers();
            Optional<DBClusterMember> clusterWriter = dbClusterMembers.stream()
                    .filter(DBClusterMember::isClusterWriter)
                    .findFirst();

            String primary = clusterWriter.map(DBClusterMember::getDBInstanceIdentifier).orElse("");
            List<String> replicas = dbClusterMembers.stream()
                    .filter(dbClusterMember -> !dbClusterMember.isClusterWriter())
                    .map(DBClusterMember::getDBInstanceIdentifier)
                    .collect(Collectors.toList());

            DescribeDBInstancesRequest describeDBInstancesRequest = new DescribeDBInstancesRequest()
                    .withFilters(Collections.singletonList(
                            new Filter()
                                    .withName("db-cluster-id")
                                    .withValues(dbCluster.getDBClusterIdentifier())));

            DescribeDBInstancesResult describeDBInstancesResult = neptune
                    .describeDBInstances(describeDBInstancesRequest);

            Collection<NeptuneInstanceProperties> instances = new ArrayList<>();
            describeDBInstancesResult.getDBInstances()
                    .forEach(c -> {
                                String role = "unknown";
                                if (primary.equals(c.getDBInstanceIdentifier())) {
                                    role = "writer";
                                }
                                if (replicas.contains(c.getDBInstanceIdentifier())) {
                                    role = "reader";
                                }
                                instances.add(
                                        new NeptuneInstanceProperties(
                                                c.getDBInstanceIdentifier(),
                                                role,
                                                c.getEndpoint().getAddress(),
                                                c.getDBInstanceStatus(),
                                                c.getAvailabilityZone(),
                                                c.getDBInstanceClass(),
                                                getTags(c.getDBInstanceArn(), neptune)));
                            }
                    );

            neptune.shutdown();

            Map<EndpointsSelector, Collection<String>> results = new HashMap<>();

            for (EndpointsSelector selector : selectors) {
                results.put(selector, selector.getEndpoints(clusterEndpoint, readerEndpoint, instances));
            }

            previousResults.set(results);

            return results;

        } catch (AmazonNeptuneException e) {
            if (e.getErrorCode().equals("Throttling")) {
                Map<EndpointsSelector, Collection<String>> results = previousResults.get();
                if (results != null) {
                    logger.warn("Calls to the Neptune Management API are being throttled. Reduce the refresh rate and stagger refresh agent requests, or use a NeptuneEndpointsInfoLambda proxy.");
                    return results;
                } else {
                    throw e;
                }
            } else {
                throw e;
            }
        }
    }

    private Map<String, String> getTags(String dbInstanceArn, AmazonNeptune neptune) {
        if (instanceTags.containsKey(dbInstanceArn)) {
            return instanceTags.get(dbInstanceArn);
        }

        List<Tag> tagList = neptune.listTagsForResource(
                new ListTagsForResourceRequest()
                        .withResourceName(dbInstanceArn)).getTagList();

        Map<String, String> tags = new HashMap<>();
        tagList.forEach(t -> tags.put(t.getKey(), t.getValue()));

        instanceTags.put(dbInstanceArn, tags);

        return tags;
    }
}
