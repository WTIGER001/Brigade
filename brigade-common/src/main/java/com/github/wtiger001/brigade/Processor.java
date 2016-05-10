package com.github.wtiger001.brigade;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The core concept in Brigade is the idea of a processor. A processor is a
 * single step of processing in an event driven orchestration. A Brigade
 * Scheduler is created to satisfy a single processor and a processor object
 * describes the
 *
 */
public class Processor {

    private static final Logger LOG = LoggerFactory.getLogger(Processor.class);

    /**
     * The name of the processor. This must be unique in the system and may use
     * dot notation
     */
    private String name;

    private DockerConfig extractor;

    /**
     * The Kafka Topic that will be monitored, When a message is detect on this
     * topic then this processor will be called.
     */
    private String inputTopic;

    /**
     * The Kafka Topic that the output of the process (docker image) will be
     * sent to if the docker process exits normally. This can be used to chain
     * together processing jobs
     */
    private String outputTopic;

    /**
     * The Kafka Topic that the output of the process (docker image) will be
     * sent to if the docker process exits abnormally (nonzero)
     */
    private String errorTopic;

    private List<Constraint> constraints;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public DockerConfig getExtractor() {
        return extractor;
    }

    public void setExtractor(DockerConfig extractor) {
        this.extractor = extractor;
    }

    @JsonProperty(value = "input", required = true)
    public String getInputTopic() {
        return inputTopic;
    }

    public void setInputTopic(String inputTopic) {
        this.inputTopic = inputTopic;
    }

    @JsonProperty(value = "output", required = true)
    public String getOutputTopic() {
        return outputTopic;
    }

    public void setOutputTopic(String outputTopic) {
        this.outputTopic = outputTopic;
    }

    @JsonProperty(value = "error", required = true)
    public String getErrorTopic() {
        return errorTopic;
    }

    public void setErrorTopic(String errorTopic) {
        this.errorTopic = errorTopic;
    }

    public List<Constraint> getConstraints() {
        return constraints;
    }

    public void setConstraints(List<Constraint> constraints) {
        this.constraints = constraints;
    }

    public String toJson() throws JsonProcessingException {
        ObjectMapper om = new ObjectMapper();
        return om.writeValueAsString(this);
    }

    public static Processor fromFile(File f) throws IOException {
        ObjectMapper om = new ObjectMapper();
        return om.readValue(f, Processor.class);
    }

    public static Processor fromStream(InputStream is) throws IOException {
        ObjectMapper om = new ObjectMapper();
        return om.readValue(is, Processor.class);
    }
    
    public static Processor fromString(String s) throws IOException {
        ObjectMapper om = new ObjectMapper();
        return om.readValue(s, Processor.class);
    }

}
