package com.github.wtiger001.brigade;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Scanner;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Represents the configuration information. This information is kept in one or
 * more JSON Formatted files. The file location can be specified at startup.
 * Each argument can either be a file or a directory. Each file that is
 * specified is watched for changes to just the file. For each directory then
 * any file that ends in .json will be loaded and the entire directory will be
 * watched. 
 * 
 */
public class Configuration {
	public String kafkaAddress;
	public String mesosMaster;
	public String frameworkName = "Ingest Manager";
	public int kafkaPollIntervalms = 100;
	public int maxInstances = 1000;
	private Collection<Processor> processors = new ArrayList<>();
	private Set<String> inputTopics = new HashSet<>();

	public void readConfiguration(String... path) throws FileNotFoundException {
		processors.clear();
		inputTopics.clear();

		for (String p : path) {
			readConfigurationFile(p);
		}

		Set<String> inputs = new HashSet<>();
		for (Processor p : processors) {
			if (p.input != null) {
				boolean added = inputs.add(p.input);
				System.out.println(added + " " + inputTopics.size());
			}
		}
		inputTopics = Collections.unmodifiableSet(inputs);
	}

	private void readConfigurationFile(String path) throws FileNotFoundException {
		String content = "";
		try (Scanner s = new Scanner(new File(path))) {
			content = s.useDelimiter("\\Z").next();
		}
		JSONObject json = new JSONObject(content);

		kafkaAddress = str(json, "kakfa-address", kafkaAddress);
		mesosMaster = str(json, "mesos-master", mesosMaster);
		frameworkName = str(json, "framework-name", frameworkName);
		kafkaPollIntervalms = intg(json, "kafka-poll-interval", kafkaPollIntervalms);
		maxInstances = intg(json, "max-instances", maxInstances);

		JSONArray arr = json.getJSONArray("processors");
		for (int i = 0; i < arr.length(); i++) {
			processors.add(new Processor(arr.getJSONObject(i)));
		}
	}
	
	public Set<String> getAllTopics() {
		return inputTopics;
	}

	public List<String> getTopicList() {
		return new ArrayList<String>(inputTopics);
	}

	public Iterable<Processor> getApplicableProcessors(String topic) {
		Collection<Processor> found = new ArrayList<>();
		for (Processor p : processors) {
			if (p.input != null && p.input.equals(topic)) {
				found.add(p);
			}
		}
		return found;
	}

	private static String str(JSONObject obj, String name, String defaultValue) {
		if (obj.has(name) && obj.get(name) != null) {
			return obj.getString(name);
		} else {
			return defaultValue;
		}
	}

	private static int intg(JSONObject obj, String name, int defaultValue) {
		if (obj.has(name) && obj.get(name) != null) {
			return obj.getInt(name);
		} else {
			return defaultValue;
		}
	}

	private static double dbl(JSONObject obj, String name, double defaultValue) {
		if (obj.has(name) && obj.get(name) != null) {
			return obj.getDouble(name);
		} else {
			return defaultValue;
		}
	}

	private static class Condition {
		public Condition(String conditionStr) {
			if (conditionStr.startsWith("$.") == false) {
				throw new RuntimeException("Must start with $.");
			}

		}

		public boolean eval(String json) {

			return true;
		}

	}

	public Processor getProcessor(String processorName) {
		for (Processor p : processors) {
			if (p.name.equalsIgnoreCase(processorName)) {
				return p;
			}
		}
		return null;
	}
	
}
