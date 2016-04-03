package com.github.wtiger001.brigade.framework;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.TopicPartition;

import com.github.wtiger001.brigade.Processor;

public class KafkaTopicTracker {
	private final String topic;
	private KafkaConsumer<String, String> consumer;
	private HashMap<TopicPartition, KafkaMessageTracker> partitionTrackers;
	private HashMap<String, KafkaMessageTracker> taskIndex;
	private KafkaInput source;
	
	public KafkaTopicTracker(String topic, KafkaInput source) {
		this.topic = topic;
		this.source = source;
		this.partitionTrackers = new HashMap<>();
		this.taskIndex = new HashMap<>();
		
		Set<TopicPartition> partitions = source.getConsumer().assignment();
		for (TopicPartition p : partitions) {
			partitionTrackers.put(p, new KafkaMessageTracker(p));
		}
	}
	
	public String getTopic() {
		return topic;
	}
	
	public void revokeTopics(Collection<TopicPartition> partitions) {
		for (TopicPartition p : partitions) {
			partitionTrackers.remove(p);
		}
	}
	
	public void updateTopics(Collection<TopicPartition> partitions) {
		for (TopicPartition p : partitions) {
			if (partitionTrackers.containsKey(p) == false) {
				partitionTrackers.put(p, new KafkaMessageTracker(p));
			}
		}
	}
	
	public void reportReceived(TopicPartition tp, long offset, String taskId) {
		KafkaMessageTracker t = partitionTrackers.get(tp);
		if (t == null) {
			throw new IllegalStateException("Recieved a Message from a Topic/Partition that " +
					"is not known about... The state of the partition is suspect [" 
					+ tp.topic() + "][" + tp.partition() +"]");
		}
		taskIndex.put(taskId, t);
		t.reportRecieved(offset, taskId);
	}
	
	public void reportSubmitted(String taskId, String hostname) {
		KafkaMessageTracker t = taskIndex.get(taskId);
		if (t == null) {
			throw new IllegalStateException("Submitted a task that is unknown [" + taskId +"]");
		}
		t.reportSubmitted(taskId, hostname);
	}
	
	public void reportDone(String taskId) {
		KafkaMessageTracker t = taskIndex.get(taskId);
		if (t == null) {
			throw new IllegalStateException("Completed a task that is unknown [" + taskId +"]");
		}
		t.reportDone(taskId);
		updateIfNeeded(t);
	}
	
	public void reportError(String taskId) {
		KafkaMessageTracker t = taskIndex.get(taskId);
		if (t == null) {
			throw new IllegalStateException("Errored a task that is unknown [" + taskId +"]");
		}
		t.reportError(taskId);
		updateIfNeeded(t);
	}
	
	private void updateIfNeeded(KafkaMessageTracker tracker) {
		OffsetAndMetadata offset = tracker.needsUpdate();
		if( offset != null) {
			source.addUpdate(tracker.getPartition(), offset);
		}
	}

	public String getHost(String taskId) {
		KafkaMessageTracker t = taskIndex.get(taskId);
		if (t == null) {
			throw new IllegalStateException("Errored a task that is unknown [" + taskId +"]");
		}
		return t.getHost(taskId);
	}

	public void reportFailure(String taskId) {
		KafkaMessageTracker t = taskIndex.get(taskId);
		if (t == null) {
			throw new IllegalStateException("Failed a task that is unknown [" + taskId +"]");
		}
		t.reportFail(taskId);
		updateIfNeeded(t);
	}

	public void updateOffset(TopicPartition partition, OffsetAndMetadata offset) {
		KafkaMessageTracker t = partitionTrackers.get(partition);
		if (t != null) {
			t.setCommitedOffset(offset.offset());
		}
	}

}
