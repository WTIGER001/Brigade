package com.github.wtiger001.brigade.general;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import org.json.JSONArray;
import org.json.JSONObject;

import com.github.wtiger001.brigade.Configuration;
import com.github.wtiger001.brigade.Processor;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.ClientResponse.Status;
import com.sun.jersey.api.client.UniformInterfaceException;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

public class Marathon {
	private static final int OK = 200;

	private String address;
	private String groupForApps;
	private Map<String, JSONObject> currentJobs = new HashMap<>();

	public Marathon(String marathonAddress, String groupForApps) {
		this.address = marathonAddress.endsWith("/") ? marathonAddress.substring(0, marathonAddress.length() - 1)
				: marathonAddress;
		this.groupForApps = groupForApps.toLowerCase();
		
		if (this.groupForApps.startsWith("/") == false) {
			this.groupForApps = "/" + this.groupForApps;
		}
	}

	public String appId(Processor p) {
		return (groupForApps +"/" + p.name +"-scheduler").toLowerCase();
	}
	
	public JSONObject makeJob( Processor p, BrigadeGeneralConfiguration configuration) {
		JSONObject obj = new JSONObject();
		
		String envProcessor = p.toJson().replaceAll("\"", "'");
		
		obj.put("id", appId(p));
		obj.put("instances", 1);
		obj.put("mem", 128);
		obj.put("cpus", 1);
		
		JSONObject container = new JSONObject();
		container.put("type", "DOCKER");
		
		JSONObject docker = new JSONObject();
		docker.put("image", configuration.schedulerImage);
		docker.put("privileged", false);
	
		container.put("docker", docker);
		obj.put("container", container);
		
		JSONObject env = new JSONObject();
		env.put(Configuration.MESOS_MASTER_ENV, configuration.mesosMaster);
		env.put(Configuration.KAFKA_ADDRESS_ENV, configuration.kafkaAddress);
		env.put(Configuration.BRIGADE_EXECUTOR_ENV, configuration.executorCommand);
		env.put(Configuration.PROCESSOR_ENV, envProcessor);
		
		obj.put("env", env);
		
		JSONObject labels = new JSONObject();
		labels.put("created-by", "brigade-general");
		
		obj.put("labels", labels);
		
		return obj;
	}
	
	public void addJob(JSONObject job) throws MarathonException {
		ClientResponse response = post(url("/v2/apps"), job.toString() );
		System.out.println(response);
		if (isOk(response)) {
		} else {
			throw new MarathonException("APP NOT CREATED!");
		}
	}
	
	public void delete(String id) {
		String url = url("/v2/apps" + id);
		System.out.println("DELETE URL : " + url);
		ClientConfig config = new DefaultClientConfig();
		Client client = Client.create(config);
		WebResource service = client.resource(UriBuilder.fromUri(url).build());
		service.delete();
	}

	private String url(String part) {
		return address + part;
	}

	public boolean groupExists(String groupId) {
		
		String url = url("/v2/groups" + groupId);
		
		System.out.println("Checking for group existence");
		String results = null;
		try {
			results = getJson(url);
		} catch (UniformInterfaceException uie) {
			return false;
		}
		
		System.out.println(results);
		
		JSONObject obj = new JSONObject(results);
		
		currentJobs.clear();
		
		if (obj.has("id")) {
			System.out.println("Group does exist");
			JSONArray jobs = obj.getJSONArray("apps");
			System.out.println("Apps in Group : " + jobs.length());
			for (int i =0; i<jobs.length(); i++) {
				JSONObject j = jobs.getJSONObject(i);
				String jId = j.getString("id");
				currentJobs.put(jId, j);
			}
			
			return true;
		} else {
			return false;
		}
	}

	public boolean createGroup(String groupId) {
		String url = url("/v2/groups");
		
		JSONObject json = new JSONObject();
		json.put("id", groupId);
		
		ClientResponse response = post(url, json.toString());
		System.out.println(response);
		if (isOk(response)) {
			return true;
		} else {
			return false;
		}
		 
	}

	
	private boolean isOk(ClientResponse response) {
		return response.getStatus() >= 200 && response.getStatus() <300;
	}
	
	private String getJson(String url) {
		System.out.println("GET URL : " + url);
		
		ClientConfig config = new DefaultClientConfig();
		Client client = Client.create(config);
		WebResource service = client.resource(UriBuilder.fromUri(url).build());
		service.accept(MediaType.APPLICATION_JSON);
		return service.get(String.class);
	}

	private ClientResponse post(String url, String formData) {
		System.out.println("POST URL : " + url);
		System.out.println(formData);
		
		ClientConfig config = new DefaultClientConfig();
		Client client = Client.create(config);
		WebResource webResource = client.resource(UriBuilder.fromUri(url).build());

		ClientResponse response = webResource.type(MediaType.APPLICATION_JSON)
				.post(ClientResponse.class, formData);

		return response;

	}
	private ClientResponse put(String url, String formData) {
		System.out.println("PUT URL : " + url);
		System.out.println(formData);
		
		ClientConfig config = new DefaultClientConfig();
		Client client = Client.create(config);
		WebResource webResource = client.resource(UriBuilder.fromUri(url).build());

		ClientResponse response = webResource.type(MediaType.APPLICATION_JSON)
				.put(ClientResponse.class, formData);

		return response;

	}
	public void makeGroup() {
		boolean exists = groupExists(groupForApps);
		if (exists == false) {
			createGroup(groupForApps);
		}
	}

	public Map<String, JSONObject> getAllProcessorJobs() {
		return currentJobs;
	}

	public void updateJob(JSONObject job) throws MarathonException {
		String id = job.getString("id");
		ClientResponse response = put(url("/v2/apps" + id + "?force=true"), job.toString() );
		System.out.println(response);
		if (isOk(response)) {
		} else {
			throw new MarathonException("APP NOT UPDATED!");
		}
	}

}
