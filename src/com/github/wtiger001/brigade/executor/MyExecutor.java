package com.github.wtiger001.brigade.executor;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.mesos.Executor;
import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkInfo;
import org.apache.mesos.Protos.SlaveInfo;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;

public class MyExecutor implements Executor {

	private Map<String, ProcessRunner> runningTasks = new ConcurrentHashMap<>();
	
	
	public static void main(String[] args) {
		MyExecutor exe = new MyExecutor();
		MesosExecutorDriver driver = new MesosExecutorDriver(exe);
		driver.run();
	}
	
	@Override
	public void registered(ExecutorDriver driver, ExecutorInfo executorInfo,
			FrameworkInfo frameworkInfo, SlaveInfo slaveInfo) {

		System.out.println("Registered the Exectutor");
		System.out.println("Executor : " + executorInfo);
		System.out.println("Framework : " + frameworkInfo);
		System.out.println("SlaveInfo : " + slaveInfo);
	}

	
	@Override
	public void disconnected(ExecutorDriver driver) {
		System.out.println("Diconnected the Exectutor");
	}

	@Override
	public void error(ExecutorDriver driver, String error) {
		System.out.println("Error in the Exectutor [" + error +"]");
	}

	@Override
	public void frameworkMessage(ExecutorDriver driver, byte[] message) {
		System.out.println("Message in the Exectutor [" + new String(message) +"]");
	}

	@Override
	public void killTask(ExecutorDriver driver, TaskID taskId) {
		System.out.println("Kill Task in the Exectutor [" + taskId.getValue() +"]");
		
		ProcessRunner runner = runningTasks.get(taskId.getValue());
		if (runner != null) {
			runner.kill();
		}
	}

	@Override
	public void launchTask(ExecutorDriver driver, TaskInfo taskInfo) {
		System.out.println("Launching Task in the Exectutor [" + taskInfo.getName() +"]");
		
		ProcessRunner runner = new ProcessRunner(driver, taskInfo, this);
		
		runningTasks.put(taskInfo.getTaskId().getValue(), runner);
		
		runner.start();
	}

	
	@Override
	public void reregistered(ExecutorDriver driver, SlaveInfo slaveInfo) {
		System.out.println("Reregistered the Exectutor");
	}

	@Override
	public void shutdown(ExecutorDriver driver) {
		System.out.println("Shutting Down the Exectutor");
	}

	public void removeRunning(String taskId) {
		runningTasks.remove(taskId);
	}

}
