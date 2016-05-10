package com.github.wtiger001.brigade;

import java.io.IOException;
import javax.annotation.PostConstruct;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.util.ResourceUtils;

/**
 * Represents the configuration information. T
 */

@org.springframework.context.annotation.Configuration
@ConfigurationProperties(prefix = "configuration")
public class Configuration {
    
    private Processor processor;
    
    private DockerConfig executor;
    
    @Value("${processor.config.location}")
    private String processorConfigLocation;
    
    @Value("${executor.config.location}")
    private String executorConfigLocation;
      
    
    @Bean
    public Processor getProcessor() {
        return processor;
    }

    @Bean
    public DockerConfig getExeuctor() {
        return executor;
    }
        
    @PostConstruct
    public void init() {
        try {
            this.processor = Processor.fromFile(ResourceUtils.getFile(processorConfigLocation));
            this.executor = DockerConfig.fromFile(ResourceUtils.getFile(executorConfigLocation));
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
    
    @Value("${kafka.endpoint}")
    private String kafkaEndpoint;
    
    @Value("${mesos.master.endpoint}")
    private String mesosMasterEndpoint;
    
    @Value("${framework.name}")
    private String frameworkName;

    public String getKafkaEndpoint() {
        return kafkaEndpoint;
    }

    public String getMesosMasterEndpoint() {
        return mesosMasterEndpoint;
    }

    public String getFrameworkName() {
        return frameworkName;
    }

    @Bean
    public static PropertySourcesPlaceholderConfigurer propertyPlaceholderConfigurer() {
        return new PropertySourcesPlaceholderConfigurer();
    }
    
}
