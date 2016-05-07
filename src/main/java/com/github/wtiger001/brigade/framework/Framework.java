package com.github.wtiger001.brigade.framework;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.mesos.MesosSchedulerDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Label;
import org.apache.mesos.Protos.Labels;
import org.apache.mesos.Protos.Status;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.mesos.Scheduler;

import com.github.wtiger001.brigade.Configuration;
import com.github.wtiger001.brigade.Processor;
import com.google.protobuf.ByteString;
import com.netflix.fenzo.SchedulingResult;
import com.netflix.fenzo.TaskAssignmentResult;
import com.netflix.fenzo.TaskScheduler;
import com.netflix.fenzo.VMAssignmentResult;
import com.netflix.fenzo.VirtualMachineLease;
import com.netflix.fenzo.functions.Action1;

/**
 * A Per Processor Framework runs a single {@link Processor}.
 */
public class Framework {

    private static final Logger LOG = LoggerFactory.getLogger(Framework.class);

    private static String VERSION = "0.0.1";

    /**
     * Thread to run the scheduler
     */
    private Thread schedulerThread;

    /**
     * Thread to run the Kafka Consumer
     */
    private Thread kafkaInputThread;

    /**
     * Thread to run the framework (task submission)
     */
    private Thread frameworkThread;

    /**
     * Thread to run the Kafka Producer
     */
    private Thread kakfaOutputThread;

    /**
     * Virtual Machine Leases that are use by Fenzo
     */
    private final BlockingQueue<VirtualMachineLease> leasesQueue;

    /**
     * The Scheduling Driver that is used to talk with mesos
     */
    private final MesosSchedulerDriver mesosSchedulerDriver;

    /**
     * The Task scheduler from Fenzo
     */
    private final TaskScheduler scheduler;

    /**
     * The Soldier executor
     */
    private final ExecutorInfo executor;

    /**
     * The Scheduling Driver reference that is used to talk with mesos
     */
    private final AtomicReference<MesosSchedulerDriver> ref = new AtomicReference<>();

    /**
     * Handles all Kafka input operations
     */
    private final KafkaInput input;

    /**
     * Handles all Kafka output operations
     */
    private final KafkaOutput output;

    /**
     * Flag to signal that everything should shut down
     */
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);

    /**
     * The tasks that need to be scheduled
     */
    private final Map<String, ProcessorTask> pendingTasksMap = new HashMap<>();

    /**
     * The Framework ID provided by the Mesos Master
     */
    private FrameworkID frameworkId;

    /**
     * Configuration that came from environment variables
     */
    private final Configuration configuration;

    /**
     * Constructor for the framework
     *
     * @param processor The processor that this framework is responsible for
     * @param configuration Configuration that came from environment variables
     */
    public Framework(Processor processor, Configuration configuration) {
        this.configuration = configuration;
        this.leasesQueue = new LinkedBlockingQueue<>();

        scheduler = new TaskScheduler.Builder().withLeaseOfferExpirySecs(10)
                .withLeaseRejectAction(new Action1<VirtualMachineLease>() {
                    @Override
                    public void call(VirtualMachineLease lease) {
                        LOG.trace("Declining offer on " + lease.hostname());
                        ref.get().declineOffer(lease.getOffer().getId());
                    }
                }).build();

        // Construct the Framework
        String frameworkName = configuration.frameworkName + "_" + processor.name + "_" + VERSION;
        Protos.FrameworkInfo framework = Protos.FrameworkInfo.newBuilder().setName(frameworkName).setUser("").build();

        // Build the Kafka components
        input = new KafkaInput(this, configuration, processor);
        output = new KafkaOutput(this, configuration, processor);

        // Build the executor
        CommandInfo ci = CommandInfo.newBuilder()
                .setValue("java -jar /battalion-1.0-SNAPSHOT-jar-with-dependencies.jar").build();

        Protos.ContainerInfo.DockerInfo di = Protos.ContainerInfo.DockerInfo.newBuilder()
                .setImage("docker.devlab.local/battalion").build();

        Protos.ContainerInfo coi = Protos.ContainerInfo.newBuilder()
                .setType(Protos.ContainerInfo.Type.DOCKER)
                .setDocker(di).build();

        executor = ExecutorInfo.newBuilder()
                .setExecutorId(ExecutorID.newBuilder().setValue(configuration.frameworkName + " Battalion Executor"))
                //                .setFrameworkId(frameworkId)
                .addResources(Protos.Resource.newBuilder().setName("cpus").setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(0.5)))
                .addResources(Protos.Resource.newBuilder().setName("mem").setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(200)))
                .setCommand(ci)
                .setContainer(coi).build();

        // Build the Scheduler
        Scheduler mesosScheduler = new MesosScheduler(scheduler, leasesQueue, input, this);
        mesosSchedulerDriver = new MesosSchedulerDriver(mesosScheduler, framework, configuration.mesosMaster);
        ref.set(mesosSchedulerDriver);
    }

    public void start() {
        // Start the Mesos Driver
        schedulerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                mesosSchedulerDriver.run();
            }
        }, "Mesos-Scheduler");
        schedulerThread.start();

        // Start the Framework
        frameworkThread = new Thread(new Runnable() {
            @Override
            public void run() {
                runAll();
            }
        }, "Task Scheduler");
        frameworkThread.start();

        // Start the Kafka Data Source
        kafkaInputThread = new Thread(input, "Kafka Input");
        kafkaInputThread.start();

        kakfaOutputThread = new Thread(output, "Kafka Output");
        kakfaOutputThread.start();
    }

    public int join() {
        Status s = mesosSchedulerDriver.join();

        return s.getNumber();
    }

    void runAll() {
        System.out.println("Running Task Scheduler");

        List<VirtualMachineLease> newLeases = new ArrayList<>();
        while (true) {
            if (isShutdown.get()) {
                return;
            }
            newLeases.clear();
            List<ProcessorTask> newTaskRequests = new ArrayList<>();

            // System.out.println("#Pending tasks: " + pendingTasksMap.size());
            ProcessorTask taskRequest = null;
            try {
                taskRequest = pendingTasksMap.size() == 0 ? input.getTaskQueue().poll(5, TimeUnit.SECONDS)
                        : input.getTaskQueue().poll(1, TimeUnit.MILLISECONDS);
            } catch (InterruptedException ie) {
                System.err.println("Error polling task queue: " + ie.getMessage());
            }
            if (taskRequest != null) {
                input.getTaskQueue().drainTo(newTaskRequests);
                newTaskRequests.add(0, taskRequest);
                for (ProcessorTask request : newTaskRequests) {
                    pendingTasksMap.put(request.getId(), request);
                }
            }
            leasesQueue.drainTo(newLeases);
            SchedulingResult schedulingResult = scheduler.scheduleOnce(new ArrayList<>(pendingTasksMap.values()),
                    newLeases);
            // System.out.println("result=" + schedulingResult);
            Map<String, VMAssignmentResult> resultMap = schedulingResult.getResultMap();
            if (!resultMap.isEmpty()) {
                for (VMAssignmentResult result : resultMap.values()) {
                    List<VirtualMachineLease> leasesUsed = result.getLeasesUsed();
                    List<Protos.TaskInfo> taskInfos = new ArrayList<>();
                    StringBuilder stringBuilder = new StringBuilder(
                            "Launching on VM " + leasesUsed.get(0).hostname() + " tasks ");
                    final Protos.SlaveID slaveId = leasesUsed.get(0).getOffer().getSlaveId();
                    for (TaskAssignmentResult t : result.getTasksAssigned()) {
                        stringBuilder.append(t.getTaskId()).append(", ");
                        taskInfos.add(getTaskInfo(slaveId, t));
                        // remove task from pending tasks map and put into
                        // launched tasks map
                        // (in real world, transition the task state)
                        pendingTasksMap.remove(t.getTaskId());
                        scheduler.getTaskAssigner().call(t.getRequest(), leasesUsed.get(0).hostname());
                        input.reportSubmitted(t.getTaskId(), leasesUsed.get(0).hostname());
                    }
                    List<Protos.OfferID> offerIDs = new ArrayList<>();
                    for (VirtualMachineLease l : leasesUsed) {
                        offerIDs.add(l.getOffer().getId());
                    }
                    LOG.trace(stringBuilder.toString());
                    mesosSchedulerDriver.launchTasks(offerIDs, taskInfos);
                }
            }

            // insert a short delay before scheduling any new tasks or tasks
            // from before that haven't been launched yet.
            try {
                Thread.sleep(100);
            } catch (InterruptedException ie) {
            }
        }
    }

    private Protos.TaskInfo getTaskInfo(Protos.SlaveID slaveID, final TaskAssignmentResult result) {
        ProcessorTask t = (ProcessorTask) result.getRequest();

        Protos.TaskID pTaskId = Protos.TaskID.newBuilder().setValue(t.getId()).build();

        // DockerInfo docker = DockerInfo.newBuilder()
        // .setImage(t.getProcessor().docker)
        // .build();
        //
        // ContainerInfo container = ContainerInfo.newBuilder()
        // .setDocker(docker)
        // .setType(ContainerInfo.Type.DOCKER)
        // .build();
//
//		ExecutorID eid = ExecutorID.newBuilder().setValue(t.getId()).build();
//
//		CommandInfo ci = CommandInfo.newBuilder().setValue(configuration.executorCommand).build();
//
//		ExecutorInfo executor = ExecutorInfo.newBuilder().setExecutorId(eid).setFrameworkId(frameworkId)
//				// .setContainer(container)
//				.setCommand(ci).build();
        ByteString data = ByteString.copyFromUtf8(t.getMessage());

        Label processorLabel = Label.newBuilder()
                .setKey("processor")
                .setValue(t.getProcessor().toJson())
                .build();

        Label messageLabel = Label.newBuilder().setKey("message").setValueBytes(data).build();
        Labels labels = Labels.newBuilder().addLabels(processorLabel).addLabels(messageLabel).build();

        return Protos.TaskInfo.newBuilder().setName("task " + pTaskId.getValue()).setTaskId(pTaskId).setSlaveId(slaveID)
                .setLabels(labels).setData(data)
                .addResources(Protos.Resource.newBuilder().setName("cpus").setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(t.getCPUs())))
                .addResources(Protos.Resource.newBuilder().setName("mem").setType(Protos.Value.Type.SCALAR)
                        .setScalar(Protos.Value.Scalar.newBuilder().setValue(t.getMemory())))
                // .setContainer(container)
                // .setCommand(Protos.CommandInfo.newBuilder().setShell(false))
                .setExecutor(executor).build();
    }

    public void setFrameworkId(FrameworkID value) {
        this.frameworkId = value;
    }

    public KafkaOutput getOutput() {
        return output;
    }
}
