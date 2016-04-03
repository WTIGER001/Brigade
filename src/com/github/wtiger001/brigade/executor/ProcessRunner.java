package com.github.wtiger001.brigade.executor;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.mesos.ExecutorDriver;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;

import com.google.protobuf.ByteString;

public class ProcessRunner extends Thread {
	
	private Process process;
	
	private ExecutorDriver driver;

	private TaskInfo taskInfo;
	
	private List<String> commands;

	private String message;

	private MyExecutor exe;

	private String taskId;
	
	private AtomicBoolean kill = new AtomicBoolean(false);
	
	public ProcessRunner(ExecutorDriver driver, TaskInfo info, MyExecutor myExecutor) {
		this.driver = driver;
		this.taskInfo = info;
		this.commands = Docker.buildCommandList(info);
		this.message = taskInfo.getData().toStringUtf8();
		this.exe = myExecutor;
		this.taskId = taskInfo.getTaskId().getValue();
	}
	
	@Override
	public void run() {
		sendStatus(TaskState.TASK_STARTING);
		
		System.out.println("MESSAGE INPUT");
		System.out.println(message);
		
		StringBuffer inBuffer = new StringBuffer();
		StringBuffer errBuffer = new StringBuffer();
		
		try {
			ProcessBuilder builder = new ProcessBuilder(commands);
			builder.redirectErrorStream(false);

			process = builder.start();
			
			OutputStream stdIn = process.getOutputStream();
			stdIn.write(message.getBytes());
			stdIn.close();
			
			InputStream inStream = process.getInputStream();
			new InputStreamHandler( inBuffer, inStream );
			
			InputStream errStream = process.getErrorStream();
			new InputStreamHandler( errBuffer , errStream );
			
			while (process.isAlive()) {
				process.waitFor(100, TimeUnit.MILLISECONDS);
				if (kill.get()) {
					process.destroy();
					sendKilled();
					return;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
			sendError(e.getMessage());
			exe.removeRunning(taskId);
			return;
			
		} catch (InterruptedException e) {
			e.printStackTrace();
			sendError(e.getMessage());
			exe.removeRunning(taskId);
			return;
		}
		
		if (kill.get() == false) {
			int exitValue = process.exitValue();
			if (exitValue == 0) {
				String response = inBuffer.toString();
				System.out.println("RESPONSE");
				System.out.println(response);
				
				sendFinished(response);
			} else {
				sendFailed("Failed to complete with exit code of " + exitValue);
			}
			exe.removeRunning(taskId);
		}
	}
	
	public void kill() {
		if (process != null)  {
			sendKilling();
			kill.set(true);
		}
	}
	
	private TaskStatus.Builder buildState() {
		TaskStatus.Builder b = TaskStatus.newBuilder()
				.setTaskId(taskInfo.getTaskId());
		
		return b;
	}
	
	private void sendError(String error) {
		TaskStatus status = buildState()
				.setHealthy(false)
				.setState(TaskState.TASK_ERROR)
				.setMessage(error)
				.build();
		
		driver.sendStatusUpdate(status);
	}
	
	private void sendFailed(String failedMessage) {
		TaskStatus status = buildState()
				.setHealthy(false)
				.setState(TaskState.TASK_FAILED)
				.setMessage(failedMessage)
				.build();
		
		driver.sendStatusUpdate(status);
	}
	
	private void sendFinished(String result) {
		TaskStatus status = buildState()
				.setHealthy(false)
				.setState(TaskState.TASK_FINISHED)
				.setData(ByteString.copyFromUtf8(result))
				.build();
		
		driver.sendStatusUpdate(status);
	}
	
	private void sendKilled() {
		TaskStatus status = buildState()
				.setHealthy(false)
				.setState(TaskState.TASK_KILLED)
				.build();
		
		driver.sendStatusUpdate(status);
	}
	
	private void sendKilling() {
		TaskStatus status = buildState()
				.setHealthy(false)
				.setState(TaskState.TASK_KILLED)
				.build();
		
		driver.sendStatusUpdate(status);
	}
	
	private void sendStatus(TaskState state) {
		sendStatus(state, null);
	}
	
	private void sendStatus(TaskState state, String response) {
		TaskStatus.Builder b = TaskStatus.newBuilder()
				.setHealthy(true)
				.setState(state)
				.setTaskId(taskInfo.getTaskId());
		
		if (response != null) {
			b.setData(ByteString.copyFromUtf8(response));
		}
		TaskStatus status = b.build();
		
		driver.sendStatusUpdate(status);
	}
	
	private static class InputStreamHandler extends Thread
	{
	 /**
	  * Stream being read
	  */
			
	 private InputStream m_stream;
			
	 /**
	  * The StringBuffer holding the captured output
	  */
			
	 private StringBuffer m_captureBuffer;
			
	 /**
	  * Constructor. 
	  * 
	  * @param 
	  */
			
	 InputStreamHandler( StringBuffer captureBuffer, InputStream stream )
	 {
	  m_stream = stream;
	  m_captureBuffer = captureBuffer;
	  start();
	 }
			
	 /**
	  * Stream the data.
	  */
			
	 public void run()
	 {
	  try
	  {
	   int nextChar;
	   while( (nextChar = m_stream.read()) != -1 )
	   {
	    m_captureBuffer.append((char)nextChar);
	   }
	  }
	  catch( IOException ioe )
	  {
	  }
	 }
	}
	
	public static void main(String[] args) {
		Process process;
		String message = "SAMPLE"; 
//		String command = "/usr/bin/docker run --name hi echod";
//		String command = "/usr/bin/docker";
		ArrayList<String> command = new ArrayList<>();
		command.add("/usr/bin/docker");
		command.add("run");
		command.add("--name");
		command.add("1234");
		command.add("echod");
		
		StringBuffer inBuffer = new StringBuffer();
		StringBuffer errBuffer = new StringBuffer();
		
		try {
			ProcessBuilder builder = new ProcessBuilder(command);
			builder.redirectErrorStream(false);

			process = builder.start();
			
			OutputStream stdIn = process.getOutputStream();
			stdIn.write(message.getBytes());
			stdIn.close();
			
			InputStream inStream = process.getInputStream();
			new InputStreamHandler( inBuffer, inStream );
			
			InputStream errStream = process.getErrorStream();
			new InputStreamHandler( errBuffer , errStream );
			
				process.waitFor();
		} catch (IOException e) {
			e.printStackTrace();
			return;
			
		} catch (InterruptedException e) {
			e.printStackTrace();
			return;
		}
		
			int exitValue = process.exitValue();
			System.out.println("EXIT " + exitValue);
			if (exitValue == 0) {
				String response = inBuffer.toString();
				System.out.println("RESPONSE");
				System.out.println(response);
				
			} else {
				
			}
	}
}
