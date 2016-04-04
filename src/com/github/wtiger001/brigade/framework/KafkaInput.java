package com.github.wtiger001.brigade.framework;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;
import org.apache.mesos.Protos.TaskStatus;

import com.github.wtiger001.brigade.Configuration;
import com.github.wtiger001.brigade.Processor;

/**
 * Source of all tasks. This class connects to a Kafka Topic and places tasks in
 * a FIFO queue to be consumed by the Framework. The configuration for this 
 * class determines how many messages are kept
 * 
 */
public class KafkaInput implements Runnable, ConsumerRebalanceListener, OffsetCommitCallback{
	private static int KAFKA_POLL_INTERVAL = 100;
	private Map<TopicPartition, OffsetAndMetadata> updates;
	private BlockingQueue<ProcessorTask> taskQueue;
	private KafkaConsumer<String, String> consumer;
	private String topic;
	private AtomicBoolean isShutdown;
	private int maxSize = 100;
	private int sleepMs = 100;
	private KafkaTopicTracker tracker;
	private final Processor processor;
	private final Framework framework;
	
	public KafkaInput(Framework framework, Configuration configuration, Processor processor) {
		this.framework = framework;
		this.processor = processor;
		this.topic = processor.input;
		this.taskQueue  = new LinkedBlockingQueue<>();
		this.isShutdown = new AtomicBoolean(false);
		this.updates = new ConcurrentHashMap<>();
		
		Properties props = new Properties();
		props.put("bootstrap.servers", configuration.kafkaAddress);
		props.put("group.id", configuration.frameworkName + "-" + topic +"-"+processor.name);
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer",	"org.apache.kafka.common.serialization.StringDeserializer");
		
		System.out.println("Connecting to Kafka topic: " + processor.input);
		this.consumer = new KafkaConsumer<>(props);
		List<String> topics = new ArrayList<>();
		topics.add(topic);
		consumer.subscribe(topics, this);
		this.tracker = new KafkaTopicTracker(topic, this);
	}

	public void shutdown() {
		isShutdown.set(true);
		consumer.wakeup();
	}
	
	@Override
	public void run() {
		while (true) {
			// Check for shutdown
			if (isShutdown.get()) {
				return;
			}
			
			// Update Kakfa with messages
			if (updates.isEmpty() == false) {
				consumer.commitAsync(updates, this);
				updates.clear();
			}
			
			// Check size
			if (taskQueue.size() < maxSize) {
//				System.out.println("Checking for new messages");
				
				// Get from Kafka
				ConsumerRecords<String, String> records = consumer.poll(KAFKA_POLL_INTERVAL);
				System.out.println("Found: " + records.count());
				
				for (ConsumerRecord<String, String> record : records) {
					// Generate the task Id
        			String id = genTaskId(record, processor);
        			
        			System.out.println("Generating Task: " + id);
        			
        			// Report the task received
					tracker.reportReceived(
							new TopicPartition(record.topic(), record.partition()),
							record.offset(), id);
  			
        			// Convert to a Task
        			ProcessorTask taskRequest = new ProcessorTask(processor, id, record.value());
        			
        			// Add to the queue
        			taskQueue.offer(taskRequest);
				}
			} else {
				System.out.println("Queue is full... Waiting a little bit");
				try {
					Thread.sleep(sleepMs);
				} catch (InterruptedException ie) {
					return;
				}
			}
		}
	}
	
	private final static String genTaskId(ConsumerRecord<String, String> record,
			Processor p) {
		return p.name + "_" + record.topic() + "_" + record.partition() + "_" + record.offset();
	}

	public void updateStatus(TaskStatus status) {
		switch (status.getState()) {
		case TASK_ERROR:
			tracker.reportError(status.getTaskId().getValue());
			break;
		case TASK_FAILED:
			tracker.reportFailure(status.getTaskId().getValue());
			break;
		case TASK_FINISHED:
			tracker.reportDone(status.getTaskId().getValue());
			
			String response = status.getData().toStringUtf8();
			System.out.println("RECEIVED RESPONSE ");
			System.out.println(response);
			
			framework.getOutput().post(response);
			
			break;
		case TASK_LOST:
			//TODO Put at the front of the queue
			System.out.println("TASK LOST <DETAILS>");
			break;
		default:
			break;
		}
	}
	
	public BlockingQueue<ProcessorTask> getTaskQueue() {
		return taskQueue;
	}

	public String getHost(String taskId) {
		return tracker.getHost(taskId);
	}

	public void reportReceived(TopicPartition tp, long offset, String taskId) {
		tracker.reportReceived(tp, offset, taskId);
	}

	public void reportSubmitted(String taskId, String hostname) {
		tracker.reportSubmitted(taskId, hostname);
	}

	@Override
	public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
		System.out.println("onPartitionsRevoked");
		for (TopicPartition p : partitions) {
			System.out.println("\t" + p.topic() + "\t"+p.partition());
		}
		tracker.revokeTopics(partitions);
	}

	@Override
	public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
		// TODO Auto-generated method stub
		System.out.println("onPartitionsAssigned");
		for (TopicPartition p : partitions) {
			System.out.println("\t" + p.topic() + "\t"+p.partition());
		}
		tracker.updateTopics(partitions);
	}

	public void addUpdate(TopicPartition partition, OffsetAndMetadata offset) {
		updates.put(partition, offset);
	}

	@Override
	public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets,	Exception exception) {
		if (exception != null) {
			System.out.println("EXCEPTION: "+ exception.getMessage());
		}
		
		for (Entry<TopicPartition, OffsetAndMetadata> entry : offsets.entrySet()) {
			System.out.println("Offsets updated! " + entry.getKey().topic() + "-" + entry.getKey().partition() + " offset: " + entry.getValue().offset());
			tracker.updateOffset(entry.getKey(), entry.getValue());
		}
	}

	public KafkaConsumer<String, String> getConsumer() {
		return consumer;
	}
	
	
}
