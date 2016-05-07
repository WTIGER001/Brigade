package com.github.wtiger001.brigade.test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaMessageGenerator {
	private static int numIters = 1;
	private static int numTasks = 3;
	private static KafkaProducer<String, String> producer;
	private static String topic = "test2";
	private static long tasks = 0;
	
	/**
	 * @param args
	 * @throws ExecutionException 
	 * @throws InterruptedException 
	 */
	public static void main(String[] args) throws InterruptedException, ExecutionException {
		// Connect to Kafka
		Properties props = new Properties();
		 
		props.put("bootstrap.servers", "mesosmaster:9092");
//		props.put("bootstrap.servers", "192.168.1.117:2181/kafka");
		props.put("request.timeout.ms", 10000);
		 props.put("acks", "all");
		 props.put("retries", 0);
		 props.put("batch.size", 1);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		producer = new KafkaProducer<>(props);
		
		for (int i = 0; i < numIters; i++) {
//		while (true) {
            for (int j = 0; j < numTasks; j++) {
            	generateTask(topic);
            	tasks++;
//            	System.out.println("Task Created " + tasks);
            }
//            System.out.println("<----- Done Iteration");
            if (numIters > 1) {
	            try {
	                Thread.sleep(10000);
	            } catch (InterruptedException ie) {
	            }
            }
        }
		System.out.println("Task Created " + tasks);
	}
	
	private static void generateTask(String topic) throws InterruptedException, ExecutionException {
		String message = "{ 'metadata' : { 'name':'abc.txt' }, 'locations' : [{ 'url':'file:///home/john/files/abc.txt', 'type':'ingest', 'processing':'none'}]}";
		message = message.replaceAll("'", "\"");
		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, message);
		producer.send(record).get();
	}

}
