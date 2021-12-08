package com.varobank.kafka.neptune;

import com.varobank.common.gremlin.utils.ConnectionCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.annotation.EnableKafka;

import javax.annotation.PreDestroy;


@SpringBootApplication
@EnableKafka
@ComponentScan("com.varobank")
public class SpringBootWithKafkaApplication {
	private static final Logger logger = LoggerFactory.getLogger(SpringBootWithKafkaApplication.class);

	@Autowired
	private ConnectionCluster cluster;

	public static void main(String[] args) {
		logger.debug("Application starting... ");
		SpringApplication.run(SpringBootWithKafkaApplication.class, args);
		logger.debug("Application launched. ");
	}

	@PreDestroy
	public void onExit() throws Exception {
		if (cluster.isCreated()) {
			cluster.close();
			logger.info("Closed Neptune cluster connection on application shutdown");
		}
	}
}
