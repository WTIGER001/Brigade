package com.github.wtiger001.brigade.framework;

import java.util.Collection;
import java.util.HashMap;
import java.util.Set;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaTopicTracker {
	private static final Logger LOG = LoggerFactory.getLogger(KafkaTopicTracker.class);
	
	private final String topic;
	private final HashMap<TopicPartition, KafkaMessageTracker> partitionTrackers;
	private final HashMap<String, KafkaMessageTracker> taskIndex;
	private final KafkaInput source;
	
	public KafkaTopicTracker(String topic, KafkaInput source) {
		LOG.info("Tracking " + topic);
		this.topic = topic;
		this.source = source;
		this.partitionTrackers = new HashMap<>();
		this.taskIndex = new HashMap<>();
		
		updateTopics(source.getConsumer().assignment());
	}


    public HashMap<String, KafkaMessageTracker> getTaskIndex() {
        return taskIndex;
    }
	
	public String getTopic() {
		return topic;
	}
	
	public void revokeTopics(Collection<TopicPartition> partitions) {
		for (TopicPartition p : partitions) {
			partitionTrackers.remove(p);
		}
	}
	
	public final void updateTopics(Collection<TopicPartition> partitions) {
            partitions.stream().filter((p) -> (!partitionTrackers.containsKey(p))).forEach((p) -> {
                partitionTrackers.put(p, new KafkaMessageTracker(p));
            });
	}
	
	public void reportReceived(TopicPartition tp, long offset, String taskId) {
		KafkaMessageTracker t = partitionTrackers.get(tp);
		if (t == null) {
			throw new IllegalStateException("Recieved a Message from a Topic/Partition that " +
					"is not known about... The state of the partition is suspect [" 
					+ tp.topic() + "][" + tp.partition() +"]");
		}
                LOG.info("Task received: {}", taskId);
		taskIndex.put(taskId, t);
		t.reportRecieved(offset, taskId);
	}
	
	public void reportSubmitted(String taskId, String hostname) {
		KafkaMessageTracker t = taskIndex.get(taskId);
		if (t == null) {
			throw new IllegalStateException("Submitted a task that is unknown [" + taskId +"]");
		}
                LOG.info("Task submitted to {}: {}", hostname, taskId);
		t.reportSubmitted(taskId, hostname);
	}
	
	public void reportDone(String taskId) {
		KafkaMessageTracker t = taskIndex.remove(taskId);
		if (t == null) {
			throw new IllegalStateException("Completed a task that is unknown [" + taskId +"]");
		}
                LOG.info("Task completed: {}", taskId);
		t.reportDone(taskId);
		updateIfNeeded(t);
	}
	
	public void reportError(String taskId) {
		KafkaMessageTracker t = taskIndex.remove(taskId);
		if (t == null) {
			throw new IllegalStateException("Errored a task that is unknown [" + taskId +"]");
		}
                LOG.info("Task errored: {}", taskId);
		t.reportError(taskId);
		updateIfNeeded(t);
	}
	
	private void updateIfNeeded(KafkaMessageTracker tracker) {
		OffsetAndMetadata offset = tracker.needsUpdate();
		if( offset != null) {
			source.addUpdate(tracker.getPartition(), offset);
		}
	}


	public void reportFailure(String taskId) {
		KafkaMessageTracker t = taskIndex.remove(taskId);
		if (t == null) {
			throw new IllegalStateException("Failed a task that is unknown [" + taskId +"]");
		}
                LOG.info("Task failed: {}", taskId);
		t.reportFail(taskId);
		updateIfNeeded(t);
	}

	public String getHost(String taskId) {
		KafkaMessageTracker t = taskIndex.get(taskId);
		if (t == null) {
			throw new IllegalStateException("Errored a task that is unknown [" + taskId +"]");
		}
		return t.getHost(taskId);
	}
        
	public void updateOffset(TopicPartition partition, OffsetAndMetadata offset) {
		KafkaMessageTracker t = partitionTrackers.get(partition);
		if (t != null) {
			t.setCommitedOffset(offset.offset());
		}
	}

}
