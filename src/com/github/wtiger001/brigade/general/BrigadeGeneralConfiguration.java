package com.github.wtiger001.brigade.general;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class BrigadeGeneralConfiguration {
	public static final String MESOS_MASTER = "MESOS_MASTER";
	public static final String KAFKA_ADDRESS= "KAFKA_ADDRESS";
	public static final String BRIGADE_EXECUTOR = "BRIGADE_EXECUTOR";
	public static final String MARATHON_ADDRESS = "MARATHON_ADDRESS";
	public static final String GROUP_ID = "GROUP_ID";
	public static final String SCHEDULER_IMAGE = "SCHEDULER_IMAGE";
	public static final String PROCESSOR_DIR = "PROCESSOR_DIR";
	
	public String mesosMaster;
	public String kafkaAddress;
	public String frameworkName = "BrigadeProc";
	public String executorCommand = "java -jar /home/john/exe/executor.jar";
	public String marathonAddress;
	public String groupId = "Brigade";
	public String schedulerImage = "brigadepreprocessor";
	public String processorDir = "/etc/brigade/processors";
	
	public BrigadeGeneralConfiguration() {
		Map<String, String> env = System.getenv();
		
		if (env.containsKey(MESOS_MASTER)) {
			mesosMaster = env.get(MESOS_MASTER);
		}
		if (env.containsKey(KAFKA_ADDRESS)) {
			kafkaAddress = env.get(KAFKA_ADDRESS);
		}
		if (env.containsKey(BRIGADE_EXECUTOR)) {
			executorCommand = env.get(BRIGADE_EXECUTOR);
		}
		if (env.containsKey(MARATHON_ADDRESS)) {
			marathonAddress = env.get(MARATHON_ADDRESS);
		}
		if (env.containsKey(SCHEDULER_IMAGE)) {
			schedulerImage = env.get(SCHEDULER_IMAGE);
		}
		if (env.containsKey(GROUP_ID)) {
			groupId = env.get(GROUP_ID);
		}
		if (env.containsKey(PROCESSOR_DIR)) {
			processorDir = env.get(PROCESSOR_DIR);
		}
	}
	
	public void fromFile(String propertiesFile) throws IOException{
		Properties props = new Properties();
		props.load(new FileInputStream(propertiesFile));
		
		if (props.containsKey(MESOS_MASTER)) {
			mesosMaster = props.getProperty(MESOS_MASTER);
		}
		if (props.containsKey(KAFKA_ADDRESS)) {
			kafkaAddress = props.getProperty(KAFKA_ADDRESS);
		}
		if (props.containsKey(BRIGADE_EXECUTOR)) {
			executorCommand = props.getProperty(BRIGADE_EXECUTOR);
		}
		if (props.containsKey(MARATHON_ADDRESS)) {
			marathonAddress = props.getProperty(MARATHON_ADDRESS);
		}
		if (props.containsKey(SCHEDULER_IMAGE)) {
			schedulerImage = props.getProperty(SCHEDULER_IMAGE);
		}
		if (props.containsKey(GROUP_ID)) {
			groupId = props.getProperty(GROUP_ID);
		}
		if (props.containsKey(PROCESSOR_DIR)) {
			processorDir = props.getProperty(PROCESSOR_DIR);
		}
	}
	
	@Override
	public String toString() {
		return "BrigadeGeneralConfiguration [mesosMaster=" + mesosMaster + ", kafkaAddress=" + kafkaAddress
				+ ", frameworkName=" + frameworkName + ", executorCommand=" + executorCommand + ", marathonAddress="
				+ marathonAddress + ", groupId=" + groupId + ", schedulerImage=" + schedulerImage + ", processorDir="
				+ processorDir + "]";
	}

	public void validate() {
		List<String> errors = new ArrayList<>();
		if (mesosMaster == null || mesosMaster.isEmpty()) {
			errors.add("MESOS_MASTER cannot be empty");
		}
		if (kafkaAddress == null || kafkaAddress.isEmpty()) {
			errors.add("KAFKA_ADDRESS cannot be empty");
		}
		if (executorCommand == null || executorCommand.isEmpty()) {
			errors.add("BRIGADE_EXECUTOR cannot be empty");
		}
		if (marathonAddress == null || marathonAddress.isEmpty()) {
			errors.add("MARATHON_ADDRESS cannot be empty");
		}
		if (groupId == null || groupId.isEmpty()) {
			errors.add("GROUP_ID cannot be empty");
		}
		if (schedulerImage == null || schedulerImage.isEmpty()) {
			errors.add("SCHEDULER_IMAGE cannot be empty");
		}
		if (kafkaAddress == null || processorDir.isEmpty()) {
			errors.add("PROCESSOR_DIR cannot be empty");
		}
		
		if (errors.isEmpty() == false) {
			String errorStr = String.join(", ", errors);
			throw new IllegalStateException(errorStr);
		}
	}
	
	
	
}
