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
	private static final String BOOTSTRAP = "BOOTSTRAP";
	private static  int sleepTime =  10* 1000; // 5 min
	private static final FilenameFilter JSON_FILTER = new FilenameFilter() {
		public boolean accept(File arg0, String arg1) {
			return arg1.toLowerCase().endsWith(".json");
		};
	};

	public static void main(String[] args) throws IOException {
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
		System.out.println("Starting with config: " + configFile==null?"NONE":configFile);
		BrigadeGeneralConfiguration cfg = new BrigadeGeneralConfiguration();
		if (configFile != null)  {
			System.out.println(cfg);
			cfg.fromFile(configFile);
		}
		System.out.println(cfg);
		cfg.validate();
		System.out.println("VALID");
		
		while (true) {
			// Load the Configuration from TBD
			Set<Processor> current = loadProcessors(cfg.processorDir);
			
			for (Processor p : current) {
				System.out.println(p.toJson());
			}
			
			
			try {
				process(cfg, current);
			} catch (MarathonException me) {
				System.err.println("Error loading marathon : " + me.getMessage());
				me.printStackTrace();
			}
			
			if (bootstrapmode) {
				// Add the general process
				
				System.out.println("COMPLETEING Bootstrap process. " 
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
					System.err.println("Error Processing File: " + ioe.getMessage());
				}
			}
		} else if (dir.isFile()) {
			try {
				Set<Processor> local = loadProcessorFile(dir);
				found.addAll(local);
			} catch (IOException ioe) {
				System.err.println("Error Processing File: " + ioe.getMessage());
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
			System.out.println("MAKE JOB");
			System.out.println(newJob.toString(2));
			
			String id = m.appId(p);
			System.out.println("Looking for id : " + id);
			JSONObject curJob = current.get(id);

			if (curJob == null) {
				System.out.println("No Job to Compare to... adding new job");
				m.addJob(newJob);
			} else if (isDifferent(newJob, curJob)) {
				System.out.println("Job Found but is different... updating job");
				m.updateJob(newJob);
			} else {
				System.out.println("Job Found but is the same");
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
		
		System.out.println(p1.toJson());
		System.out.println(p2.toJson());
		
		if (p1.equals(p2)) {
			return false;
		}

		return true;
	}

}
