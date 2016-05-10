package com.github.wtiger001.brigade.framework;

import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.github.wtiger001.brigade.Configuration;
import com.github.wtiger001.brigade.Processor;
import javax.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class KafkaOutput implements Runnable {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaOutput.class);

    @Autowired
    private Processor processor;

    @Autowired
    private Configuration configuration;

    private final BlockingQueue<ProducerRecord<String, String>> requests = new LinkedBlockingQueue<>();

    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    private KafkaProducer<String, String> producer;

    @PostConstruct
    public void init() {

        Properties props = new Properties();

        props.put("bootstrap.servers", configuration.getKafkaEndpoint());
        props.put("request.timeout.ms", 10000);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 1);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

    public void post(String message) {
        if (processor.getOutputTopic() != null && processor.getOutputTopic().isEmpty() == false) {
            requests.add(new ProducerRecord<>(processor.getOutputTopic(), message));
        }
    }

    @Override
    public void run() {
        while (true) {
            if (shutdown.get()) {
                return;
            }

            ProducerRecord<String, String> request;
            try {
                request = requests.poll(100, TimeUnit.MILLISECONDS);
                if (request != null) {
                    producer.send(request);
                    LOG.debug("-output sent: " + request);
                }
            } catch (InterruptedException e) {
                LOG.error(e.getMessage(), e);
                return;
            }
        }

    }

    public void postError(String message) {
        if (processor.getErrorTopic() != null && processor.getErrorTopic().isEmpty() == false) {
            requests.add(new ProducerRecord<>(processor.getErrorTopic(), message));
        }
    }

}
