/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.github.wtiger001.brigade;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 *
 * @author bmcdonie
 */
public class DockerConfig {

    private String dockerImage;
    private double memory;
    private double cpus;
    private Boolean forcePull;
    private List<String> env;
    private List<Volume> volumes;

    @JsonProperty(required = false)
    public List<Volume> getVolumes() {
        return volumes;
    }

    public void setVolumes(List<Volume> volumes) {
        this.volumes = volumes;
    }
    
    public String getDockerImage() {
        return dockerImage;
    }

    public void setDockerImage(String dockerImage) {
        this.dockerImage = dockerImage;
    }

    @JsonProperty(value = "memory", defaultValue = "64.0")
    public double getMemory() {
        return memory;
    }

    public void setMemory(double memory) {
        this.memory = memory;
    }

    @JsonProperty(value = "cpus", defaultValue = "0.5")
    public double getCpus() {
        return cpus;
    }

    public void setCpus(double cpus) {
        this.cpus = cpus;
    }

    @JsonProperty(value = "forcePull", defaultValue = "false")
    public Boolean getForcePull() {
        return forcePull;
    }

    public void setForcePull(Boolean forcePull) {
        this.forcePull = forcePull;
    }

    public List<String> getEnv() {
        return env;
    }

    public void setEnv(List<String> env) {
        this.env = env;
    }
    public static DockerConfig fromFile(File f) throws IOException {
        ObjectMapper om = new ObjectMapper();
        return om.readValue(f, DockerConfig.class);
    }
}
