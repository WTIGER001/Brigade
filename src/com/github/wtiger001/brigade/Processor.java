package com.github.wtiger001.brigade;

import org.json.JSONObject;

public class Processor {
	public String name;
	public double memory;
	public double cpus;
	public String docker;
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		long temp;
		temp = Double.doubleToLongBits(cpus);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((docker == null) ? 0 : docker.hashCode());
		result = prime * result + ((input == null) ? 0 : input.hashCode());
		temp = Double.doubleToLongBits(memory);
		result = prime * result + (int) (temp ^ (temp >>> 32));
		result = prime * result + ((name == null) ? 0 : name.hashCode());
		result = prime * result + ((output == null) ? 0 : output.hashCode());
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
		Processor other = (Processor) obj;
		if (Double.doubleToLongBits(cpus) != Double.doubleToLongBits(other.cpus))
			return false;
		if (docker == null) {
			if (other.docker != null)
				return false;
		} else if (!docker.equals(other.docker))
			return false;
		if (input == null) {
			if (other.input != null)
				return false;
		} else if (!input.equals(other.input))
			return false;
		if (Double.doubleToLongBits(memory) != Double.doubleToLongBits(other.memory))
			return false;
		if (name == null) {
			if (other.name != null)
				return false;
		} else if (!name.equals(other.name))
			return false;
		if (output == null) {
			if (other.output != null)
				return false;
		} else if (!output.equals(other.output))
			return false;
		return true;
	}

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
