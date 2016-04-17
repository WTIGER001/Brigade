package com.github.wtiger001.brigade;

import org.json.JSONObject;

/**
 * Represents a Volume Bind-Mount in Docker
 */
public class Volume {
	
	/**
	 * The host path (the path on the OS that you want docker to access). This can be a directory or a single file
	 */
	public String hostPath;
	
	/**
	 * How the path is accessible in the docker
	 */
	public String containerPath;
	
	/**
	 * RW or RO modes
	 */
	public String mode;
	
	public Volume(JSONObject obj) {
		if (obj.has("host-path")) {
			hostPath = obj.getString("host-path");
		}
		if (obj.has("container-path")) {
			containerPath = obj.getString("container-path");
		}
		if (obj.has("mode")) {
			mode = obj.getString("mode");
		}
	}
	
	
	public JSONObject toJsonObject() {
		JSONObject obj = new JSONObject();
		
		if (hostPath != null) {
			obj.put("host-path", hostPath);
		}
		if (containerPath != null) {
			obj.put("container-path", containerPath);
		}
		if (mode != null) {
			obj.put("mode", mode);
		}
		
		return obj;
	}
}
