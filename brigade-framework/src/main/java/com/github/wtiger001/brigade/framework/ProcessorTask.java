package com.github.wtiger001.brigade.framework;

import java.util.Collections;
import java.util.List;

import com.github.wtiger001.brigade.Processor;
import com.netflix.fenzo.ConstraintEvaluator;
import com.netflix.fenzo.TaskRequest;
import com.netflix.fenzo.VMTaskFitnessCalculator;
import com.netflix.fenzo.plugins.HostAttrValueConstraint;
import java.util.ArrayList;

public class ProcessorTask implements TaskRequest {

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        ProcessorTask other = (ProcessorTask) obj;
        if (id == null) {
            if (other.id != null) {
                return false;
            }
        } else if (!id.equals(other.id)) {
            return false;
        }
        return true;
    }

    private final Processor p;
    private final String id;
    private final String message;

    public ProcessorTask(Processor p, String id, String message) {
        this.p = p;
        this.id = id;
        this.message = message;
    }

    public Processor getProcessor() {
        return p;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public double getCPUs() {
        return p.getExtractor().getCpus();
    }

    @Override
    public double getDisk() {
        return 0;
    }

    @Override
    public List<? extends ConstraintEvaluator> getHardConstraints() {
        List<ConstraintEvaluator> constraints = new ArrayList<>();

        if (p.getConstraints() != null) {
            p.getConstraints().stream().forEach((c) -> {
                constraints.add(new HostAttrValueConstraint(c.getAttribute(), (String t1) -> c.getValue()));
            });
        }

        return constraints;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public double getMemory() {
        return p.getExtractor().getMemory();
    }

    @Override
    public double getNetworkMbps() {
        return 0;
    }

    @Override
    public int getPorts() {
        return 0;
    }

    @Override
    public List<? extends VMTaskFitnessCalculator> getSoftConstraints() {
        return Collections.emptyList();
    }

    @Override
    public String taskGroupName() {
        return p.getName();
    }

}
