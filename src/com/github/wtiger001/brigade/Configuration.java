package com.github.wtiger001.brigade;

/**
 * Represents the configuration information. T
 */
public class Configuration {
	public String kafkaAddress;
	public String mesosMaster;
	public String frameworkName = "BrigadeProc";
	public String executorCommand = "java -jar /home/john/exe/executor.jar";
}
