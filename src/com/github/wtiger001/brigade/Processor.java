package com.github.wtiger001.brigade;

import org.json.JSONObject;

public class Processor {
	public String name;
	public double memory;
	public double cpus;
	public String docker;
	public String input;
	public String output;
	
	public Processor() {
		
	}
	
	public Processor(JSONObject json) {
		this.name = str(json, "name", "NO_NAME");
		this.input = str(json, "input", null);
		this.output = str(json, "output", null);
		this.docker = str(json, "docker", "NO_DOCKER");
		this.memory = dbl(json, "mem", 64.0);
		this.cpus = dbl(json, "cpus", 0.2);
	}
	
	public Processor(String jsonString) {
		this(new JSONObject(jsonString));
	}
	
	private static String str(JSONObject obj, String name, String defaultValue) {
		if (obj.has(name) && obj.get(name) != null) {
			return obj.getString(name);
		} else {
			return defaultValue;
		}
	}
	
	public String toJson() {
		JSONObject json = new JSONObject();
		json.put("name", name);
		if (memory > 0) {
			json.put("mem", memory);
		}
		if (cpus > 0 ) {
			json.put("cpus", cpus);
		}
		if (input != null) {
			json.put("input", input);
		}
		if (output != null) {
			json.put("output", output);
		}
		if (docker != null) {
			json.put("docker", docker);
		}
		
		return json.toString();
	}

	private static double dbl(JSONObject obj, String name, double defaultValue) {
		if (obj.has(name) && obj.get(name) != null) {
			return obj.getDouble(name);
		} else {
			return defaultValue;
		}
	}
}
