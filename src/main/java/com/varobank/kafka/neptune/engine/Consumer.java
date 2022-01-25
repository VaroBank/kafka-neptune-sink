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
package com.varobank.kafka.neptune.engine;

import com.google.common.cache.CacheBuilder;
import com.varobank.common.gremlin.utils.InvalidSchemaException;
import com.varobank.kafka.neptune.utils.TopicUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.json.JSONException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.AbstractConsumerSeekAware;
import org.springframework.kafka.listener.BatchListenerFailedException;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.*;
import java.util.concurrent.ConcurrentMap;

@Service
public class Consumer extends AbstractConsumerSeekAware {

    private final Logger logger = LoggerFactory.getLogger(Consumer.class);

    @Autowired
    private TopicUtil topicUtil;

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private NeptuneBatchWriter neptuneBatchWriter;

    @Autowired
    private ApplicationContext applicationContext;

    public void setNeptuneBatchWriter(NeptuneBatchWriter neptuneBatchWriter) {
        this.neptuneBatchWriter = neptuneBatchWriter;
    }

    private ConcurrentMap<Long, Integer> retryRateByLatestQuarterHour =
            CacheBuilder.newBuilder().maximumSize(5L).<Long, Integer>build().asMap();

    private String getDlqTopicName(String topic) {
        return topic + "__" + getApplicationId() + "__dlq";
    }

    public String getApplicationId() {
        return applicationContext.getId();
    }

    /**
     * Batch Kafka Listener (up to 10 kafka messages). If the entire batch is successfully processed and written to Neptune
     * then the receipt of the kafka messages is acknowledged and the offset for each kafka topic is committed.
     * Otherwise the receipt is not acknowledged and the same batch of messages will be pulled again by this method.
     * @param list
     * @param ack
     * @throws Exception
     */
    @KafkaListener(id = "kafkaConsumer", topics = "#{topicUtil.topics()}"
            ,containerFactory = "kafkaBatchListenerContainerFactory")
    public void listenBatch0(List<ConsumerRecord<String, String>> list, Acknowledgment ack) throws BatchListenerFailedException {
        if (list != null || !list.isEmpty()) {
            logger.debug("polled from kafka " + list.size());
            try {
                validateSchema(list);
                writeBatchToNeptune(list);
            } catch (Exception e) {
                throw new BatchListenerFailedException("Batch listener failed", e, 0);
            }
            list.clear();
            ack.acknowledge();
        } else {
            logger.debug("polled from kafka empty list");
            ack.acknowledge();
        }
    }

    /**
     * Validates the user defined Neptune schema against the kafka messages - if not valid throws an InvalidSchemaException
     * @param list
     * @throws InvalidSchemaException
     */
    private void validateSchema(List<ConsumerRecord<String, String>> list) throws InvalidSchemaException {
        neptuneBatchWriter.validateSchema(list);
    }

    private void validateSchema(ConsumerRecord<String, String> record) throws InvalidSchemaException {
        neptuneBatchWriter.validateSchema(Arrays.asList(record));
    }

    /**
     * Tries to process the batch of kafka messages. If it fails and the exception is retriable then it will try to process those messages
     * one by one.
     * If a processing of an individual kafka message fails then such message will be sent to the __dlq kafka topic for manual review/reprocessing.
     * * Note, the rate of failed messages is expected to be very low under the normal circumstances. If the rate of failed messages is high then
     *   this method will throw an Exception - which means that the entire batch will be not acknowledged and pulled by batch kafka listener method again.
     *   High rate of failed messages means that there is either a connection issue to the Neptune DB or the user defined Neptune schema
     *   is misconfigured and has to be fixed. Until these issues are fixed none of the kafka messages will be acknowledged and kafka topic offset
     *   will not be committed.
     * @param list
     * @throws Exception
     */
    public void writeBatchToNeptune(List<ConsumerRecord<String, String>> list) throws Exception {
        try {
            neptuneBatchWriter.writeToNeptune(list);
        } catch (Exception e) {
            logger.error("Neptune error: " + e.getMessage(), e);
            if (e instanceof JSONException || e.getMessage().contains("ConstraintViolationException") ||
                    e.getMessage().contains("ConcurrentModificationException")) {
                // If batch write to Neptune fails with one of the known errors then retry writing to Neptune one by one
                // ConcurrentModificationException is the main cause of retries
                // ConstraintViolationException is under consideration here - it should not happen, need to observe for some time
                // JSONException if happens should just go to dlq for the record keeping purpose and only retried after the message itself is fixed or parser method is fixed

                // One by one write to Neptune can also fail and if fail rate is low the failed kafka messages will be sent to the dlq topic for the manual review/retry
                // If the fail rate is high (> 10 failed attempts per JVM process for the latest 15 min) then this method will not acknowledge receipt of the batch and will throw the exception
                // as a result the listener will read the same batch all over again until it can write the received data to Neptune
                boolean isRetriableBatch = allowToSendToRetryTopic();
                boolean retrySuccess = true;
                Exception lastRetryException = null;
                for (ConsumerRecord<String, String> record : list) {
                    if (record != null) {
                        try {
                            neptuneBatchWriter.retryWriteToNeptune(record);
                        } catch (Exception ex) {
                            // Counts failed attempts as per the latest 15 min
                            incrementRetryCounter();
                            if (!isRetriableBatch) {
                                // If the fail attempt limit is reached then stop processing
                                lastRetryException = ex;
                                retrySuccess = false;
                                break;
                            } else {
                                // Send the original kafka message to dlq topic to be review/retried manually
                                sendRecordToDlqTopic(record);
                            }
                        }
                    }
                }
                if (!retrySuccess) {
                    logger.error("Neptune retry failed: " + lastRetryException.getMessage(), lastRetryException);
                    Thread.sleep(4000);
                    throw lastRetryException;
                }
            } else {
                // This is non-retriable due to Neptune connection issue or some other network factors.
                // Not acknowledging this batch.
                Thread.sleep(4000);
                throw e;
            }
        }
    }

    private void sendRecordToDlqTopic(ConsumerRecord<String, String> record) {
        String topic = record.topic();
        String appId = getApplicationId();
        if (topic.endsWith("__" + appId + "__retry")) {
            topic = topic.replace("__" + appId + "__retry", "");
        }
        String message = record.value();
        sendMessage(message, getDlqTopicName(topic));
    }

    private boolean allowToSendToRetryTopic() {
        boolean allow = true;
        long currentQuarterHourHash = getCurrentQuarterHourHash();
        if (retryRateByLatestQuarterHour.containsKey(currentQuarterHourHash)) {
            if (retryRateByLatestQuarterHour.get(currentQuarterHourHash) > 10) {
                allow = false;
            }
        } else {
            retryRateByLatestQuarterHour.put(currentQuarterHourHash, 0);
        }

        return allow;
    }

    private void incrementRetryCounter() {
        long currentQuarterHourHash = getCurrentQuarterHourHash();
        if (!retryRateByLatestQuarterHour.containsKey(currentQuarterHourHash)) {
            retryRateByLatestQuarterHour.put(currentQuarterHourHash, 0);
        }
        retryRateByLatestQuarterHour.computeIfPresent(currentQuarterHourHash, (key, val) -> val + 1);
    }

    private long getCurrentQuarterHourHash() {
        return System.currentTimeMillis()/1000000;
    }

    /**
     * If there is a need to retry processing of the failed kafka messages that went to the dlq topic
     * these messages can be sent to the retry topic for example:
     *    from cdc.login.login__application__dlq => cdc.login.login__application__retry
     * This method listens to the retry kafka topics and pulls/processes those messages one by one.
     * If it fails to process a kafka message then such message will be sent again to the __dlq kafka topic.
     * @param message
     * @param ack
     */
    @KafkaListener(topics = "#{topicUtil.retryTopics()}"
            , containerFactory = "kafkaListenerContainerFactory")
    public void listenRetryVertices(ConsumerRecord<String, String> message, Acknowledgment ack) {
        if (message != null) {
            try {
                validateSchema(message);
                neptuneBatchWriter.retryWriteToNeptune(message);
                ack.acknowledge();
            } catch (JSONException jsonException) {
                logger.error("Non-retriable JSONException: " + message.topic() + " " + message.value() + jsonException);
                sendRecordToDlqTopic(message);
                ack.acknowledge();
            } catch (Exception e) {
                logger.error("Neptune retry error: " + e.getMessage(), e);
                sendRecordToDlqTopic(message);
                ack.acknowledge();
            }
        }
    }

    public void sendMessage(String message, String topic) {
        ListenableFuture<SendResult<String, String>> future =
                kafkaTemplate.send(topic, message);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

            @Override
            public void onSuccess(SendResult<String, String> result) {
                logger.info("Sent message=[" + message +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            }
            @Override
            public void onFailure(Throwable ex) {
                logger.error("Unable to send message=["
                        + message + "] due to : " + ex.getMessage());
            }
        });
        kafkaTemplate.flush();
    }

}
