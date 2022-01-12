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

package com.varobank.kafka.neptune.utils;

import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsync;
import com.amazonaws.services.cloudwatch.AmazonCloudWatchAsyncClientBuilder;
import com.amazonaws.services.cloudwatch.model.Dimension;
import com.amazonaws.services.cloudwatch.model.MetricDatum;
import com.amazonaws.services.cloudwatch.model.PutMetricDataRequest;
import com.amazonaws.services.cloudwatch.model.StandardUnit;
import com.varobank.common.gremlin.utils.ActivityTimer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Metrics {

    private static final Logger logger = LoggerFactory.getLogger(Metrics.class);

    AmazonCloudWatchAsync cwa = AmazonCloudWatchAsyncClientBuilder.defaultClient();

    private final String clusterId;
    private final List<Double> batchSizes = new ArrayList<>();
    private final List<Double> durations = new ArrayList<>();
    private final List<Double> retryCounts = new ArrayList<>();

    public Metrics(String clusterId) {
        this.clusterId = clusterId;
    }

    public void add(int batchSize, long duration, int retryCount) {
        batchSizes.add((double) batchSize);
        durations.add((double) duration);
        retryCounts.add((double) retryCount);
    }

    public void publish() {

        try (ActivityTimer timer = new ActivityTimer("Publish metrics")) {

            MetricDatum edgesSubmitted = new MetricDatum()
                    .withMetricName("EdgesSubmitted")
                    .withUnit(StandardUnit.Count)
                    .withValues(batchSizes)
                    .withStorageResolution(1)
                    .withDimensions(new Dimension().withName("clusterId").withValue(clusterId));

            MetricDatum writeDuration = new MetricDatum()
                    .withMetricName("WriteDuration")
                    .withUnit(StandardUnit.Milliseconds)
                    .withValues(durations)
                    .withStorageResolution(1)
                    .withDimensions(new Dimension().withName("clusterId").withValue(clusterId));

            MetricDatum retryCount = new MetricDatum()
                    .withMetricName("RetryCount")
                    .withUnit(StandardUnit.Count)
                    .withValues(retryCounts)
                    .withStorageResolution(1)
                    .withDimensions(new Dimension().withName("clusterId").withValue(clusterId));

            try {
                cwa.putMetricData(new PutMetricDataRequest().
                        withMetricData(edgesSubmitted, writeDuration, retryCount).
                        withNamespace("aws-samples/stream-2-neptune"));
            } catch (Exception e) {
                logger.error("Swallowed exception: " + e.getLocalizedMessage());
            }
        }
    }
}
