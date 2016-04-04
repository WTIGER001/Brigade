package com.github.wtiger001.brigade.framework;

import java.io.FileNotFoundException;
import java.util.Arrays;

import com.github.wtiger001.brigade.Configuration;

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
		if (args.length <2) {
			throw new IllegalArgumentException("Bad Args");
		}
		//TODO LOGGING!
		// Processor
		String processorName = args[0];
		System.out.println("PROCESSOR NAME : " + processorName);
		
		// Load the configuration
		String[] configurationFiles = Arrays.copyOfRange(args, 1, args.length);
		for (String c : configurationFiles) {
			System.out.println("CONFIGURATION FILE : " + c);
		}
		
		Configuration configuration = new Configuration();
		configuration.readConfiguration(configurationFiles);
		
		// Create the Framework
		Framework f = new Framework(processorName, configuration);
		f.start();
		int code = f.join();
		System.exit(code);
	}
}
