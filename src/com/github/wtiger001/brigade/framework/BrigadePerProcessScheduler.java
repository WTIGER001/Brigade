package com.github.wtiger001.brigade.framework;

import java.io.FileNotFoundException;
import java.util.ArrayList;

import com.github.wtiger001.brigade.Configuration;
import com.github.wtiger001.brigade.Processor;

/**
 * Application to start the Brigade Per Processor Scheduler Framework
 */
public class BrigadePerProcessScheduler {

	

	/*
	 * NEED TO PASS IN?: processor name and a list of config files... This needs to be completely redone
	 * 
	 * ??Can just require:
	 * 1) mesos-master
	 * 2) kakfa-address
	 * 3) processor description (json)
	 */
	public static void main(String[] args) throws FileNotFoundException {
//		if (args.length <2) {
//			throw new IllegalArgumentException("Bad Args");
//		}
		//TODO LOGGING!
		// Processor
//		String processorName = args[0];
//		System.out.println("PROCESSOR NAME : " + processorName);
//		
//		// Load the configuration
//		String[] configurationFiles = Arrays.copyOfRange(args, 1, args.length);
//		for (String c : configurationFiles) {
//			System.out.println("CONFIGURATION FILE : " + c);
//		}
//		
//		Configuration configuration = new Configuration();
//		configuration.readConfiguration(configurationFiles);
		ArrayList<String> errors = new ArrayList<>();
		String mesosmaster = System.getenv(Configuration.MESOS_MASTER_ENV);
		if (mesosmaster == null || mesosmaster.isEmpty()) {
			errors.add("Missing MESOS_MASTER environment variable");
		}
		
		String kafkaaddress = System.getenv(Configuration.KAFKA_ADDRESS_ENV);
		if (kafkaaddress == null || kafkaaddress.isEmpty()) {
			errors.add("Missing KAFKA_ADDRESS environment variable");
		}
		
		String executorCmd = System.getenv(Configuration.BRIGADE_EXECUTOR_ENV);
		if (executorCmd == null || executorCmd.isEmpty()) {
			errors.add("Missing BRIGADE_EXECUTOR environment variable");
		}
		
		String processorJson = System.getenv(Configuration.PROCESSOR_ENV);
		if (processorJson == null || processorJson.isEmpty()) {
			errors.add("Missing PROCESSOR environment variable");
		}
		
		if (errors.isEmpty() == false) {
			for (String err : errors) {
				System.err.println(err);
			}
			System.exit(1);
		}

		Processor p = new Processor(processorJson);
		
		Configuration configuration = new Configuration();
		configuration.mesosMaster = mesosmaster;
		configuration.kafkaAddress = kafkaaddress;
		configuration.executorCommand = executorCmd;
		
		
		// Create the Framework
		Framework f = new Framework(p, configuration);
		f.start();
		int code = f.join();
		System.exit(code);
	}
}
