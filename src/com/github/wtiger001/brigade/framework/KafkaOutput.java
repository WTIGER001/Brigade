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

public class KafkaOutput implements Runnable {

	private final Processor processor;
	private final BlockingQueue<ProducerRecord<String, String>> requests;
	private final AtomicBoolean shutdown = new AtomicBoolean(false);
	private final KafkaProducer<String, String> producer;

	public KafkaOutput(Framework framework, Configuration cfg, Processor processor) {
		this.processor = processor;
		this.requests = new LinkedBlockingQueue<>();
		
		Properties props = new Properties();
		 
		props.put("bootstrap.servers", cfg.kafkaAddress);
		props.put("request.timeout.ms", 10000);
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 1);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<>(props);
	}
	
	public void post(String message) {
		if (processor.output != null && processor.output.isEmpty() == false) {
			requests.add(new ProducerRecord<String, String>(processor.output, message));
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
					System.out.println("-output sent: " + request);
				} 
			} catch (InterruptedException e) {
				e.printStackTrace();
				return;
			}
		}
		
	}

	public void postError(String message) {
		if (processor.error != null && processor.error.isEmpty() == false) {
			requests.add(new ProducerRecord<String, String>(processor.error, message));
		}
	}

	
}
