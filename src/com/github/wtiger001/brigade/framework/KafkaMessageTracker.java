package com.github.wtiger001.brigade.framework;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

/**
 * Tracks the status of message reciept and processing for each of the messages for a given processor. 
 */
public class KafkaMessageTracker {
	
	private static class Job {
		META_STATUS status;
		
		long offset;
		
		String taskId;

		public String host;
		
		public Job(META_STATUS status, long offset, String taskId) {
			super();
			this.status = status;
			this.offset = offset;
			this.taskId = taskId;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + (int) (offset ^ (offset >>> 32));
			result = prime * result
					+ ((status == null) ? 0 : status.hashCode());
			result = prime * result
					+ ((taskId == null) ? 0 : taskId.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Job other = (Job) obj;
			if (offset != other.offset)
				return false;
			if (status != other.status)
				return false;
			if (taskId == null) {
				if (other.taskId != null)
					return false;
			} else if (!taskId.equals(other.taskId))
				return false;
			return true;
		}
		
		
	}
	
	private long commitedOffset;
	private Map<Long, Job> offsetIndex = new TreeMap<>();
	private Map<String, Job> taskIndex = new HashMap<>();
	private final TopicPartition partition;
	
	public KafkaMessageTracker(TopicPartition partition) {
		super();
		this.partition = partition;
	}

	private static enum META_STATUS {
		RECEIVED,
		SUBMITTED,
		SUCCESS,
		FAILURE, 
		ERROR
	}
	
	public void reportRecieved(long offset, String taskId) {
		Job j = new Job(META_STATUS.RECEIVED, offset, taskId);
		offsetIndex.put(offset, j);
		taskIndex.put(taskId, j);
	}

	public void reportSubmitted(String taskId, String hostname) {
		Job j = taskIndex.get(taskId);
		if (j == null) {
			throw new IllegalStateException("Unknown Task [" + taskId +"]");
		}
		j.host = hostname;
		j.status = META_STATUS.SUBMITTED;	
	}

	public void reportDone(String taskId) {
		Job j = taskIndex.get(taskId);
		if (j == null) {
			throw new IllegalStateException("Unknown Task [" + taskId +"]");
		}
		j.status = META_STATUS.SUCCESS;	
	}
	
	public void reportError(String taskId) {
		Job j = taskIndex.get(taskId);
		if (j == null) {
			throw new IllegalStateException("Unknown Task [" + taskId +"]");
		}
		j.status = META_STATUS.ERROR;	
	}
	
	public void reportFail(String taskId) {
		Job j = taskIndex.get(taskId);
		if (j == null) {
			throw new IllegalStateException("Unknown Task [" + taskId +"]");
		}
		j.status = META_STATUS.FAILURE;	
	}
	
	public void reportCommitedOffset(long offset) {
		this.commitedOffset = offset;
	}
	
	public long calcOffset() {
		long newOffset = -1;
		for (Job j : offsetIndex.values()) {
			System.out.println("Checking " + j.offset);
			if (j.status == META_STATUS.ERROR || j.status == META_STATUS.SUCCESS || j.status == META_STATUS.FAILURE) {
				newOffset = j.offset;
			} else {
				break;
			}
		}
		return newOffset+1;
	}

	public void trimBefore(long offset) {
		Iterator<Entry<Long, Job>> it = offsetIndex.entrySet().iterator();
		while (it.hasNext()) {
			Entry<Long, Job> e = it.next();
			if (e.getKey() < offset) {
				it.remove();
			}
		}
	}

	public TopicPartition getPartition() {
		return partition;
	}
	
	public void setCommitedOffset(long commitedOffset) {
		this.commitedOffset = commitedOffset;
	}
	
	public long getCommitedOffset() {
		return commitedOffset;
	}
	
	public OffsetAndMetadata needsUpdate() {
		long off = calcOffset();
		System.out.println("OFFSET: " + off);
		if (off > 0) {
			System.out.println("NEED TO BE UPDATED ");
			trimBefore(off);
			return new OffsetAndMetadata(off);
		}
		return null;
	}

	public String getHost(String taskId) {
		Job j = taskIndex.get(taskId);
		if (j == null) {
			throw new IllegalStateException("Unknown Task [" + taskId +"]");
		}
		return j.host;
	}
}
