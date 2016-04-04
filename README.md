# Brigade
An Apache Mesos Framework for processing messages using Kafka

## Description
Brigade is an Apache Mesos framework that monitors Kafka topics to perform processing in Docker. Brigade gets configured with a set of "processors". Each processor has an input topic, a docker to perform processing and an optional output topic where the processing results will be stored. Brigade is humorously named after the old time bucket brigades that were used to put out fires and move dirt. This project fills the need for a Mesos Nativce framework for simplistic message processing for more complex workflows Apache Storm and APache Spark will always be better candidates.

> Author's Note: Please be patient with me. This is my first Mesos Framework and I am learning the particulars of it as I go.

## Architecture

### Brigade Meta Scheduler
[Not Started Yet] The meta scheduler is a top level scheduler (that is expected to be run in Marathon). It will run a Brigade Process Scheduler for each processor in the configuration. As the configuration changes the Brigade Meta Scheduler will adapt and start or stop each Brigade Process Scheduler as necessary.The Meta Scheduler is an optional component. You can instead just configure each of the process schedulers independently.

### Brigade Process Scheduler
The Process Scheduler is responsible for handling a single processor configuration. The process scheduler starts a Apache Kafka Consumer to monitor an input topic. For each input topic the scheduler attempts to schedule tasks in Apache Mesos. It uses the Netflix Fenzo library for task determination. The message and the processor configuration are sent as information in the task for the Brigade Executor to use. The scheduler receives a TASK_FINISHED status and retrieves the output from the DOcker processor (part of the Task Status). The scheduler has an Apache Kafka Producer that then places the output message in the "output" topic for the processor. Using this approach it can be very simple to string together a processing chain.

### Processor Specification
The processor is specified in JSON. The schema for it is:

```
{
	"name"  : 	"PROCESSOR NAME",
	"input" :	"INPUT TOPIC NAME",
	"output":   "OUTPUT TOPIC NAME",
	"docker":   "DOCKER IMAGE NAME",
	"mem"	:	"MEMORY TO USE",
	"cpus"  :	"CPUS TO USE"
}
```

A sample configuration file is included. Note, long term plans are to use the Curator library to store the configuration of each processor in ZooKeeper

### Brigade Executor
The Brigade Executor is a simple executor that runs the Docker image specified for a processor. The executor expects that the processor configuration is stored as a Label in the TaskInfo proto under the key "processor-configuration" and the input message from Kafka is also stored as a Label under the key "input-message". The Executor captures the standard output and returns it in the TaskStatus proto using the "Data" element.

### Docker Processor Specification
The docker image that is used to process a message is expected to follow a 12 factor application approach. Most importantly the input message written to the standard input and the docker is expected to use that input and generate an output on Standard Out.  

## Install and Run
Current instructions. Apache Mesos and Apache Kafka must be running. 

1. Build the Executor - Build the executor and place it in either the same place for all the Mesos Agents ore place it on a commonly shared drive.
2. For each Processor - Run the "Framework" class with the arguments processor name and the configuration JSON file

## Dependancies

Gradle / Maven is not yet setup so until then please just get the necessary dependencies manually.

- fenzo-core-0.8.6.jar
- jackson-annotations-2.7.3.jar
- jackson-core-2.7.3.jar
- jackson-databind-2.7.3.jar
- json-20160212.jar
- kafka-clients-0.9.0.1-sources.jar
- kafka-clients-0.9.0.1.jar
- mesos-0.28.0.jar
- protobuf-java-2.6.1.jar
- slf4j-1.7.20.tar.gz
- slf4j-api-1.7.20.jar
- slf4j-jdk14-1.7.20.jar
- slf4j-simple-1.7.20.jar

## TODO
LOTS TO DO

### Overall
- Get Gradle or Maven working to pull in the dependencies

### Per Processor Framework
- Load configuration from zookeeper?
- Update to use executor
- Task Reconciliation
- Failure modes
- Explore more robust tracking model
- Calculate metrics
- Implement Health Checks
- Integrate FluentD
- Separate the INI type information from the processor configuration

### Executor
- Complete docker options supported and update JSON Configuration 
- Language? Try Go!
- Consider having the executor send the output message to Kafka
- Figure out how to run the executor as a docker
- Look at the docker remote API

### Meta Framework
- Write basic framework to load configuration and start “per-processor” frameworks in Marathon
- REST API to get and update configuration
- Write in? Python? Go?
- List for changes in the configuration. On a change to a processor then restart the “per-processor” framework
- Implement Health Checks
- Integrate FluentD

### User Interface
- We need one! 
- Configuration Interface (read / write)
- Metrics / Tracking Interface