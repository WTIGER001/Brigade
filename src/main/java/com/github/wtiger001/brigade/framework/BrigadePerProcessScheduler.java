package com.github.wtiger001.brigade.framework;

import java.io.FileNotFoundException;
import java.util.ArrayList;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.wtiger001.brigade.Configuration;
import com.github.wtiger001.brigade.Processor;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import org.json.JSONObject;
import sun.misc.IOUtils;

/**
 * Application to start the Brigade Per Processor Scheduler Framework
 */
public class BrigadePerProcessScheduler {

    private static final Logger LOG = LoggerFactory.getLogger(BrigadePerProcessScheduler.class);

    /**
     * Starts a Per Processor Framework for a single {@link Processor}.
     *
     * The following environment variable are REQUIRED
     * <ul>
     * <li>MESOS_MASTER - The path to the mesos master (e.g.
     * zk://192.168.1.1:2181/mesos)</li>
     * <li>KAFKA_ADDRESS - The path to the Kafka Node (e.g.
     * kafka_node_1:9092)</li>
     * <li>BRIGADE_EXECUTOR - The executabvle command to start the executor</li>
     * <li>PROCESSOR - The processor definition in JSON (see
     * {@link Processor}</li>
     * </ul>
     *
     * @param args NONE
     *
     * @throws FileNotFoundException
     */
    public static void main(String[] args) throws FileNotFoundException, IOException {

        
        
        ArrayList<String> errors = new ArrayList<>();
        String mesosmaster = System.getenv(Configuration.MESOS_MASTER_ENV);
        if (mesosmaster == null || mesosmaster.isEmpty()) {
            LOG.error("Missing MESOS_MASTER environment variable");
        }

        String kafkaaddress = System.getenv(Configuration.KAFKA_ADDRESS_ENV);
        if (kafkaaddress == null || kafkaaddress.isEmpty()) {
            LOG.error("Missing KAFKA_ADDRESS environment variable");
        }

        String executorCmd = System.getenv(Configuration.BRIGADE_EXECUTOR_ENV);
        if (executorCmd == null || executorCmd.isEmpty()) {
            LOG.error("Missing BRIGADE_EXECUTOR environment variable");
        }

        String processorJson = System.getenv(Configuration.PROCESSOR_ENV);
        if (processorJson == null || processorJson.isEmpty()) {
            LOG.error("Missing PROCESSOR environment variable");
            processorJson = org.apache.commons.io.IOUtils.toString(BrigadePerProcessScheduler.class
                    .getResourceAsStream("/sl-nitf-processor.json"));
            

        }

        if (errors.isEmpty() == false) {
            System.exit(1);
        }

        Processor p = new Processor(processorJson);

        Configuration configuration = new Configuration();
        configuration.mesosMaster = mesosmaster;
        configuration.kafkaAddress = kafkaaddress;
        configuration.executorCommand = executorCmd;

        // Create the Framework
        LOG.info("Starting for " + p.name);
        Framework f = new Framework(p, configuration);
        f.start();
        int code = f.join();
        LOG.info("Exiting " + p.name + " code : " + code);

        System.exit(code);
    }
}
