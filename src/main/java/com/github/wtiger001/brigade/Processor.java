package com.github.wtiger001.brigade;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

/**
 * The core concept in Brigade is the idea of a processor. A processor is a single step of processing
 * in an event driven orchestration. A Brigade Scheduler is created to satisfy a single processor and 
 * a processor object describes the 
 *
 */
public class Processor {
	/**
	 * The name of the processor. This must be unique in the system and may use
	 * dot notation
	 */
	public String name;

	/**
	 * The amount of memory, in MB that needs to be reserved
	 */
	public double memory;

	/**
	 * The amount of CPUS that need to be reserved for this docker process
	 */
	public double cpus;

	/**
	 * The Docker Image that needs to be fetched
	 */
	public String docker;

	/**
	 * The Kafka Topic that will be monitored, When a message is detect on this
	 * topic then this processor will be called.
	 */
	public String input;

	/**
	 * The Kafka Topic that the STDOUT of the process (docker image) will be
	 * sent to if the docker process exits normally. This can be used to chain
	 * together processing jobs
	 */
	public String output;

	/**
	 * The Kafka Topic that the STDOUT of the process (docker image) will be
	 * sent to if the docker process exits abnormally (nonzero)
	 */
	public String error;

	/**
	 * The command line for the executor that will be used. The expectation of
	 * the executor is that the executor will receive the message from a label
	 * in the TaskInfo Protobuffer and run the docker image indicated by this
	 * processor (also in a label). The executor needs a mechanism for calling
	 * the docker with the message and capturing the output and exit status. The
	 * executor then needs to return the result of the STDOUT and the proper
	 * status to the Brigade Scheduler
	 */
	public String executor;

	/**
	 * A list of docker volumes that need to be mounted by docker
	 */
	public List<Volume> volumes = new ArrayList<>();

	/**
	 * The Environment Variable needed by this processor's docker file
	 */
	public List<String> env = new ArrayList<>();

	/**
	 * The ports necessary,
	 */
	public Map<Integer, Integer> ports = new HashMap<>();

        public String hostnameConstraint;
        
        public Map<String, String> attributeConstraints = new HashMap<>();

	public Processor() {

	}

	public Processor(JSONObject json) {
		this.name = json.optString("name", "NO_NAME");
		this.docker = json.optString("docker", "NO_DOCKER");
		this.input = json.optString("input", null);
		this.output = json.optString("output", null);
		this.error = json.optString("error", null);
		this.memory = json.optDouble("mem", 64.0);
		this.cpus = json.optDouble("cpus", 0.2);

		if (json.has("volumes")) {
			JSONArray vols = json.getJSONArray("volumes");
			for (int i = 0; i < vols.length(); i++) {
				Volume v = new Volume(vols.getJSONObject(i));
				this.volumes.add(v);
			}
		}
                
                JSONObject constraints = json.optJSONObject("constraints");
                if (constraints != null) {
                    this.hostnameConstraint = constraints.optString("hostname");
                    JSONObject attributes = constraints.optJSONObject("attributes");
                    if (attributes != null) {
                        for (String s : attributes.keySet()) {
                            this.attributeConstraints.put(s, attributes.optString(s));
                        }
                    }
                    
                }

		if (json.has("env")) {
			JSONArray envs = json.getJSONArray("env");
			for (int i = 0; i < envs.length(); i++) {
				String envStatement = envs.getString(i);
				env.add(envStatement);
			}
		}

	}

	public Processor(String jsonString) {
		this(new JSONObject(jsonString));
	}

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

	public String toJson() {
		JSONObject json = new JSONObject();
		json.put("name", name);
		if (memory > 0) {
			json.put("mem", memory);
		}
		if (cpus > 0) {
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

		if (volumes.isEmpty() == false) {
			JSONArray arr = new JSONArray();
			for (Volume v : volumes) {
				arr.put(v.toJsonObject());
			}
			json.put("volumes", arr);
		}

		if (env.isEmpty() == false) {
			JSONArray arr = new JSONArray(env);
			json.put("env", arr);
		}

		return json.toString();
	}

}
