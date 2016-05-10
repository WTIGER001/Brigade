package com.github.wtiger001.brigade.general;

import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.UriBuilder;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.wtiger001.brigade.Configuration;
import com.github.wtiger001.brigade.Processor;
import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import com.sun.jersey.api.client.config.ClientConfig;
import com.sun.jersey.api.client.config.DefaultClientConfig;

/**
 * Marathon is a utility to handle operations on the Marathon V2 REST API.
 */
public class Marathon {
	private static final Logger LOG = LoggerFactory.getLogger(Marathon.class);
	private String address;
	private String groupForApps;
	private Map<String, JSONObject> currentJobs = new HashMap<>();

	/**
	 * Constructor
	 * 
	 * @param marathonAddress
	 *            The address for the Marathon REST API. TODO: Allow multiple
	 *            endpoint locations
	 * @param groupForApps
	 *            The group for all the applications to be placed in. This
	 *            should start with a '/'
	 */
	public Marathon(String marathonadd, String grp) {
		this.address = marathonadd.endsWith("/") ? marathonadd.substring(0, marathonadd.length() - 1)
				: marathonadd;
		this.groupForApps = grp.toLowerCase();

		if (this.groupForApps.startsWith("/") == false) {
			this.groupForApps = "/" + this.groupForApps;
		}
		LOG.info("Configuring Marathon: Address :" + marathonadd);
		LOG.info("Configuring Marathon: Group ID:" + groupForApps);
	}

	/**
	 * Calculates the id that will be used in marathon to uniquely identify an
	 * application.
	 * 
	 * @param p
	 *            The processor that the application will be created to handle
	 * @return The marathon alowable application id (e.g. lowercase)
	 */
	public String appId(Processor p) {
		return (groupForApps + "/" + p.getName() + "-scheduler").toLowerCase();
	}

	/**
	 * Create the JSON description for a scheduler job in marathon
	 * 
	 * @param p
	 *            The processor that the application will be created to handle
	 * @param configuration
	 *            the configuration object to use
	 * @return The JSON job ready for use in marathon
	 */
	public JSONObject makeJob(Processor p, BrigadeGeneralConfiguration configuration) throws JsonProcessingException {
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

	/**
	 * Add the job to marathon
	 * 
	 * @param job
	 *            Job to add
	 * @throws MarathonException
	 *             If there is a POST error. Most times this is either due to
	 *             marathon address being incorrect or the job being added when
	 *             it already exists
	 */
	public void addJob(JSONObject job) throws MarathonException {
		ClientResponse response = post(url("/v2/apps"), job.toString());
		LOG.trace("Status Code: " + response.getStatus());
		if (isOk(response)) {
		} else {
			throw new MarathonException("APP NOT CREATED!");
		}
	}

	/**
	 * Delete a job from marathon
	 * 
	 * @param id
	 *            the jobs unique id
	 */
	public void delete(String id) {
		String url = url("/v2/apps" + id);
		LOG.info("DELETE URL : " + url);
		ClientConfig config = new DefaultClientConfig();
		Client client = Client.create(config);
		WebResource service = client.resource(UriBuilder.fromUri(url).build());
		service.delete();
	}

	/**
	 * Update a job's configuration
	 * 
	 * @param job
	 *            Job to update
	 * @throws MarathonException
	 *             If there is a POST error. Most times this is either due to
	 *             marathon address being incorrect or the job being updated
	 *             when it does not exist
	 */
	public void updateJob(JSONObject job) throws MarathonException {
		String id = job.getString("id");
		ClientResponse response = put(url("/v2/apps" + id + "?force=true"), job.toString());
		LOG.trace("Status Code: " + response.getStatus());
		if (isOk(response)) {
		} else {
			throw new MarathonException("APP NOT UPDATED!");
		}
	}
	
	/**
	 * Checks if a group exists and if not creates it
	 */
	public void makeGroup() {
		boolean exists = groupExists(groupForApps);
		if (exists == false) {
			createGroup(groupForApps);
		}
	}
	
	/**
	 * Determines if a group exists. If the group exists then the current jobs
	 * that are in the group are stored in a cache.
	 * 
	 * @param groupId
	 *            groupid to delete
	 * @return
	 * 		true if the group exists and false if it does not
	 */
	public boolean groupExists(String groupId) {

		String url = url("/v2/groups" + groupId);

		LOG.debug("Checking for group existence");
		ClientResponse response = getJson(url);
		if (isOk(response) == false) {
			return false;
		}

		String results = response.getEntity(String.class);

		JSONObject obj = new JSONObject(results);

		currentJobs.clear();

		if (obj.has("id")) {
			LOG.trace("Group does exist");
			JSONArray jobs = obj.getJSONArray("apps");
			LOG.trace("Apps in Group : " + jobs.length());
			for (int i = 0; i < jobs.length(); i++) {
				JSONObject j = jobs.getJSONObject(i);
				String jId = j.getString("id");
				currentJobs.put(jId, j);
			}

			return true;
		} else {
			return false;
		}
	}

	/**
	 * Creates a group
	 * 
	 * @param groupId
	 *            groupid to create
	 * @return true if successful
	 */
	public boolean createGroup(String groupId) {
		String url = url("/v2/groups");

		JSONObject json = new JSONObject();
		json.put("id", groupId);

		ClientResponse response = post(url, json.toString());
		LOG.trace("Status Code: " + response.getStatus());
		if (isOk(response)) {
			return true;
		} else {
			return false;
		}
	}

	private boolean isOk(ClientResponse response) {
		return response.getStatus() >= 200 && response.getStatus() < 300;
	}

	private ClientResponse getJson(String url) {
		LOG.trace("GET URL : " + url);

		ClientConfig config = new DefaultClientConfig();
		Client client = Client.create(config);
		WebResource service = client.resource(UriBuilder.fromUri(url).build());
		service.accept(MediaType.APPLICATION_JSON);
		ClientResponse response = service.get(ClientResponse.class);

		return response;
	}

	private ClientResponse post(String url, String formData) {
		LOG.trace("POST URL : " + url);
		LOG.trace(formData);

		ClientConfig config = new DefaultClientConfig();
		Client client = Client.create(config);
		WebResource webResource = client.resource(UriBuilder.fromUri(url).build());

		ClientResponse response = webResource.type(MediaType.APPLICATION_JSON).post(ClientResponse.class, formData);

		return response;

	}

	private ClientResponse put(String url, String formData) {
		LOG.trace("PUT URL : " + url);
		LOG.trace(formData);

		ClientConfig config = new DefaultClientConfig();
		Client client = Client.create(config);
		WebResource webResource = client.resource(UriBuilder.fromUri(url).build());

		ClientResponse response = webResource.type(MediaType.APPLICATION_JSON).put(ClientResponse.class, formData);

		return response;

	}

	

	public Map<String, JSONObject> getAllProcessorJobs() {
		return currentJobs;
	}

	private String url(String part) {
		return address + part;
	}
}
