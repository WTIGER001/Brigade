package com.github.wtiger001.brigade;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Represents a Volume Bind-Mount in Docker
 */
public class Volume {

    /**
     * The host path (the path on the OS that you want docker to access). This
     * can be a directory or a single file
     */
    
    
    private String hostPath;

    /**
     * How the path is accessible in the docker
     */
    private String containerPath;

    /**
     * RW or RO modes
     */
    private String mode;

    public Volume(String hostPath, String containerPath, String mode) {
        this.hostPath = hostPath;
        this.containerPath = containerPath;
        this.mode = mode;
    }

    public Volume() {
    }

    
    
    @JsonProperty("host-path")
    public String getHostPath() {
        return hostPath;
    }

    public void setHostPath(String hostPath) {
        this.hostPath = hostPath;
    }

    @JsonProperty("container-path")
    public String getContainerPath() {
        return containerPath;
    }

    public void setContainerPath(String containerPath) {
        this.containerPath = containerPath;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }
}
