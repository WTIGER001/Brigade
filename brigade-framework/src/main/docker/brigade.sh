#!/bin/bash

export MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so
export MESOS_NATIVE_LIBRARY=/usr/lib/libmesos.so
ls /brigade*/brigade*jar | xargs java -jar
