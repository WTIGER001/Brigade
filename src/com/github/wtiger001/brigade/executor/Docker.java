package com.github.wtiger001.brigade.executor;

import java.util.ArrayList;
import java.util.List;

import org.apache.mesos.Protos.Label;
import org.apache.mesos.Protos.TaskInfo;

import com.github.wtiger001.brigade.Processor;

public class Docker {
	
	public static final List<String> buildCommandList(TaskInfo taskInfo) {
		String config = findLabel(taskInfo, "processor-configuration");
		if (config == null) {
			throw new RuntimeException("No Cnfiguration");
		}
		Processor processor = new Processor(config);
		
		
		// Build the run
		ArrayList<String> command = new ArrayList<>();
		command.add("/usr/bin/docker");
		command.add("run");
		
		// Add the name
		command.add("--name");
		command.add(taskInfo.getTaskId().getValue());
		
		// Add the environment variables
		
		
		// Add the volumes
		
		
		//Finally add the image
		command.add(processor.docker);
		return command;
	}

	public static final String buildCommand(TaskInfo taskInfo) {
		return String.join(" ", buildCommand(taskInfo));
	}

	private static String findLabel(TaskInfo taskInfo, String name) {
		for ( Label l :  taskInfo.getLabels().getLabelsList()){
			if (l.hasKey() && l.getKey().equals(name)) {
				return l.getValue();
			}
		}
		return null;
	}
	
}
