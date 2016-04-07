package com.github.wtiger001.brigade;

/**
 * Represents the configuration information. T
 */
public class Configuration {
	public static final String MESOS_MASTER_ENV = "MESOS_MASTER";
	public static final String KAFKA_ADDRESS_ENV = "KAFKA_ADDRESS";
	public static final String BRIGADE_EXECUTOR_ENV = "BRIGADE_EXECUTOR";
	public static final String PROCESSOR_ENV = "PROCESSOR";
	
	public String kafkaAddress;
	public String mesosMaster;
	public String frameworkName = "BrigadeProc";
	public String executorCommand = "java -jar /home/john/exe/executor.jar";
}
