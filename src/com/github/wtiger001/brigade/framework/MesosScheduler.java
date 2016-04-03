package com.github.wtiger001.brigade.framework;

import java.util.List;
import java.util.concurrent.BlockingQueue;

import org.apache.mesos.Protos;
import org.apache.mesos.Scheduler;
import org.apache.mesos.SchedulerDriver;

import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.plugins.VMLeaseObject;
import com.netflix.fenzo.samples.SampleFramework;

public class MesosScheduler implements Scheduler {
	private final BlockingQueue<VirtualMachineLease> leasesQueue;
	private final TaskScheduler scheduler;
	private final KafkaInput source;
	private Framework framework;

	public MesosScheduler(TaskScheduler scheduler, BlockingQueue<VirtualMachineLease> leasesQueue, KafkaInput source, Framework framework) {
		this.scheduler = scheduler;
		this.leasesQueue = leasesQueue;
		this.source = source;
		this.framework = framework;
	}
	
	/**
     * When we register successfully with mesos, any previous resource offers are invalid. Tell Fenzo scheduler
     * to expire all leases (aka offers) right away.
     */
    @Override
    public void registered(SchedulerDriver driver, Protos.FrameworkID frameworkId, Protos.MasterInfo masterInfo) {
        System.out.println("Registered! ID = " + frameworkId.getValue());
        this.framework.setFrameworkId(frameworkId);
        scheduler.expireAllLeases();
    }

    /**
     * Similar to {@code registered()} method, expire any previously known resource offers by asking Fenzo
     * scheduler to expire all leases right away.
     */
    @Override
    public void reregistered(SchedulerDriver driver, Protos.MasterInfo masterInfo) {
        System.out.println("Re-registered " + masterInfo.getId());
        scheduler.expireAllLeases();
    }

    /**
     * Add the received Mesos resource offers to the lease queue. Fenzo scheduler is used by calling its main
     * allocation routine in a loop, see {@link SampleFramework#runAll()}. Collect offers from mesos into a queue
     * so the next call to Fenzo's allocation routine can pick them up.
     */
    @Override
    public void resourceOffers(SchedulerDriver driver, List<Protos.Offer> offers) {
        for(Protos.Offer offer: offers) {
            System.out.println("Adding offer " + offer.getId() + " from host " + offer.getHostname());
            leasesQueue.offer(new VMLeaseObject(offer));
        }
    }

    /**
     * Tell Fenzo scheduler that a resource offer should be expired immediately.
     */
    @Override
    public void offerRescinded(SchedulerDriver driver, Protos.OfferID offerId) {
        scheduler.expireLease(offerId.getValue());
    }

    /**
     * Update Fenzo scheduler of task completion if received status indicates a terminal state. There is no need
     * to tell Fenzo scheduler of task started because that is supposed to have been already done before launching
     * the task in Mesos.
     *
     * In a real world framework, this state change would also be persisted with a state machine of choice.
     */
    @Override
    public void statusUpdate(SchedulerDriver driver, Protos.TaskStatus status) {
        System.out.println("Task Update: " + status.getTaskId().getValue() + " in state " + status.getState());
        switch (status.getState()) {
            case TASK_FAILED:
            case TASK_LOST:
            case TASK_FINISHED:
            case TASK_ERROR:
            	source.updateStatus(status);
            	String taskId = status.getTaskId().getValue();
            	String host = source.getHost(taskId);
            	scheduler.getTaskUnAssigner().call(status.getTaskId().getValue(), host);
            	break;
		case TASK_KILLED:
			break;
		case TASK_KILLING:
			break;
		case TASK_RUNNING:
			source.updateStatus(status);
			break;
		case TASK_STAGING:
			break;
		case TASK_STARTING:
			break;
		default:
			break;
        	
        }
    }

    @Override
    public void frameworkMessage(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, byte[] data) {}

    @Override
    public void disconnected(SchedulerDriver driver) {}

    /**
     * Upon slave lost notification, tell Fenzo scheduler to expire all leases with the given slave ID. Note, however,
     * that if there was no offer received from that slave prior to this call, Fenzo would not have a mapping from
     * the slave ID to hostname (Fenzo maintains slaves state by hostname). This is OK since there would be no offers
     * to expire. However, any tasks running on the lost slave will not be removed by this call to Fenzo. Task lost
     * status updates would ensure that.
     */
    @Override
    public void slaveLost(SchedulerDriver driver, Protos.SlaveID slaveId) {
        scheduler.expireAllLeasesByVMId(slaveId.getValue());
    }

    /**
     * Do nothing, instead, rely on task lost status updates to inform Fenzo of task completions.
     */
    @Override
    public void executorLost(SchedulerDriver driver, Protos.ExecutorID executorId, Protos.SlaveID slaveId, int status) {
        System.out.println("Executor " + executorId.getValue() + " lost, status=" + status);
    }

    @Override
    public void error(SchedulerDriver driver, String message) {}
	

}
