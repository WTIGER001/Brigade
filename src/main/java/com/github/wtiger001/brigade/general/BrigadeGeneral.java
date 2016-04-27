package com.github.wtiger001.brigade.general;

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.wtiger001.brigade.Processor;

/**
 * Brigade General is used to control all the PerProcessor schedulers. It will
 * launch and shutdown Per Processor schedulers based on the configuration
 * settings. It uses Marathon to start and stop the per processor schedulers.
 * You are expected to run this process in Marathon too.
 * 
 * The Following Environment Variables are expected: MESOS_MASTER - The address
 * to the mesos master KAFKA_ADDRESS - Where a Kafka node is located
 * BRIGADE_EXECUTOR - The command for the executor GROUP_ID - The app group id
 * in marathon to use. Defaults to "Brigade" BRIGADE_IMAGE - The imagename of
 * the scheduler (Defaults to "brigadeperprocess")
 * 
 */
public class BrigadeGeneral {
	private static final Logger LOG = LoggerFactory.getLogger(BrigadeGeneral.class);
	private static final String BOOTSTRAP = "BOOTSTRAP";
	private static  int sleepTime =  10* 1000; // 5 min
	private static final FilenameFilter JSON_FILTER = new FilenameFilter() {
		public boolean accept(File arg0, String arg1) {
			return arg1.toLowerCase().endsWith(".json");
		};
	};

	public static void main(String[] args) throws IOException {
		LOG.info("Starting BrigadeGeneral " + String.join(", ", args));
		
		String configFile = null;
		boolean bootstrapmode = false;
		if (args.length == 1) {
			if (args[0].toUpperCase().equals(BOOTSTRAP)) {
				bootstrapmode = true;
			} else {
				configFile = args[0];
			}
		} else if (args.length == 2) {
			if (args[0].toUpperCase().equals(BOOTSTRAP)) {
				bootstrapmode = true;
				configFile = args[1];
			} else if (args[1].toUpperCase().equals(BOOTSTRAP)) {
				bootstrapmode = true;
				configFile = args[0];
			} else {
				throw new IllegalArgumentException(
						"I am confused... I expected a BOOTSTRAP argument and a file argument!");
			}
		}
		
		if (configFile == null) {
			LOG.info("No configuration file specified");
		} else if (new File(configFile).canRead() == false ){
			LOG.error("Cannot read configuration file " + configFile);
			System.exit(1);
		} else {
			LOG.info("Starting with config: " + configFile);
		}
		
		BrigadeGeneralConfiguration cfg = new BrigadeGeneralConfiguration();
		if (configFile != null)  {
			cfg.fromFile(configFile);
		}
		LOG.info("Configuration Settings: " + cfg.toString());

		try {
			cfg.validate();
		} catch (IllegalStateException ie) {
			LOG.error("Invalid Configuration ", ie);
			System.exit(1);
		}
		
		while (true) {
			// Load the Configuration from TBD
			Set<Processor> current = loadProcessors(cfg.processorDir);
			
			for (Processor p : current) {
				System.out.println(p.toJson());
			}
			
			try {
				process(cfg, current);
			} catch (MarathonException me) {
				LOG.error("Error loading marathon : " + me.getMessage(), me);
			}
			
			if (bootstrapmode) {
				// Add the general process
				LOG.info("COMPLETEING Bootstrap process. " 
						+ "Brigade General should now be running in marathon");
				return;
			}
			
			try {
				Thread.sleep(sleepTime);
			} catch (InterruptedException e) {
			}
		}
	}

	private static Set<Processor> loadProcessors(String processorDir) {
		File dir = new File(processorDir);
		Set<Processor> found = new HashSet<>();

		if (dir.isDirectory()) {
			File[] files = dir.listFiles(JSON_FILTER);

			for (File f : files) {
				try {
					Set<Processor> local = loadProcessorFile(f);
					found.addAll(local);
				} catch (IOException ioe) {
					LOG.error("Error Processing File " + f.getAbsolutePath(), ioe);
				}
			}
		} else if (dir.isFile()) {
			try {
				Set<Processor> local = loadProcessorFile(dir);
				found.addAll(local);
			} catch (IOException ioe) {
				LOG.error("Error Processing File " + dir.getAbsolutePath() , ioe);
			}
		}

		return found;
	}

	private static Set<Processor> loadProcessorFile(File processorFile) throws IOException {
		String data = new String(Files.readAllBytes(processorFile.toPath()));
		data = data.trim();

		Set<Processor> found = new HashSet<>();

		if (data.startsWith("{")) {
			JSONObject obj = new JSONObject(data);
			Processor p = new Processor(obj);
			found.add(p);
		} else if (data.startsWith("[")) {
			JSONArray arr = new JSONArray(data);
			for (int i = 0; i < arr.length(); i++) {
				JSONObject obj = arr.getJSONObject(i);
				Processor p = new Processor(obj);
				found.add(p);
			}
		} else {
			throw new IllegalArgumentException("JSON File must start with either '{' or ']' for file " + processorFile);
		}
		return found;
	}

	public static void process(BrigadeGeneralConfiguration cfg,
			Collection<Processor> processors) throws MarathonException {

		Marathon m = new Marathon(cfg.marathonAddress, cfg.groupId);

		// Make the group if needed
		m.makeGroup();

		// Get all the current Jobs
		Map<String, JSONObject> current = m.getAllProcessorJobs();

		// Now add all the processors
		for (Processor p : processors) {
			// Convert the processor to a job configuration for marathon
			JSONObject newJob = m.makeJob(p,cfg);
			LOG.debug("Making Job: " + p.name);
			LOG.trace(newJob.toString(2));
			
			String id = m.appId(p);
			LOG.debug("Looking for id : " + id);
			JSONObject curJob = current.get(id);

			if (curJob == null) {
				LOG.debug("No Job to Compare to... adding new job");
				m.addJob(newJob);
			} else if (isDifferent(newJob, curJob)) {
				LOG.debug("Job Found but is different... updating job");
				m.updateJob(newJob);
			} else {
				LOG.debug("Job Found but is the same");
			}

			current.remove(id);
		}

		// Now remove the jobs that are not in the list of processors
		Iterator<JSONObject> itLeftover = current.values().iterator();
		while (itLeftover.hasNext()) {
			JSONObject old = itLeftover.next();
			String id = old.getString("id");
			m.delete(id);
		}

	}

	private static boolean isDifferent(JSONObject newJob, JSONObject json) {
		Processor p1 = new Processor(newJob.getJSONObject("env").getString("PROCESSOR"));
		Processor p2 = new Processor(json.getJSONObject("env").getString("PROCESSOR"));
		
		LOG.trace(p1.toJson());
		LOG.trace(p2.toJson());
		
		if (p1.equals(p2)) {
			return false;
		}

		return true;
	}

}
