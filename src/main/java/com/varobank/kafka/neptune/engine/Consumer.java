package com.varobank.kafka.neptune.engine;

import com.google.common.cache.CacheBuilder;
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

    @KafkaListener(id = "customerProfileConsumer", topics = "#{topicUtil.topics()}"
            ,containerFactory = "kafkaBatchListenerContainerFactory")
    public void listenBatch0(List<ConsumerRecord<String, String>> list, Acknowledgment ack) throws Exception {
        if (list != null || !list.isEmpty()) {
            logger.info("polled from kafka " + list.size());
            writeBatchToNeptune(list);
            list.clear();
            ack.acknowledge();
        } else {
            logger.info("polled from kafka empty list");
            ack.acknowledge();
        }
    }

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

    @KafkaListener(topics = "#{topicUtil.retryTopics()}"
            , containerFactory = "kafkaListenerContainerFactory")
    public void listenRetryVertices(ConsumerRecord<String, String> message, Acknowledgment ack) {
        if (message != null) {
            try {
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
                logger.info("Unable to send message=["
                        + message + "] due to : " + ex.getMessage());
            }
        });
        kafkaTemplate.flush();
    }

}
