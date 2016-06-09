/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.myriad.scheduler;

import java.util.*;

import javax.inject.Inject;

import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorID;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.myriad.configuration.MyriadConfiguration;
import org.apache.myriad.state.NodeTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates Tasks based upon Mesos offers
 */
public interface TaskFactory {

  static final String YARN_RESOURCEMANAGER_HOSTNAME = "yarn.resourcemanager.hostname";
  static final String YARN_RESOURCEMANAGER_WEBAPP_ADDRESS = "yarn.resourcemanager.webapp.address";
  static final String YARN_RESOURCEMANAGER_WEBAPP_HTTPS_ADDRESS = "yarn.resourcemanager.webapp.https.address";
  static final String YARN_HTTP_POLICY = "yarn.http.policy";
  static final String YARN_HTTP_POLICY_HTTPS_ONLY = "HTTPS_ONLY";


  TaskInfo createTask(Offer offer, FrameworkID frameworkId, TaskID taskId, NodeTask nodeTask);

  // TODO(Santosh): This is needed because the ExecutorInfo constructed
  // to launch NM needs to be specified to launch placeholder tasks for
  // yarn containers (for fine grained scaling).
  // If mesos supports just specifying the 'ExecutorId' without the full
  // ExecutorInfo, we wouldn't need this interface method.
  ExecutorInfo getExecutorInfoForSlave(FrameworkID frameworkId, Offer offer, CommandInfo commandInfo);

  /**
   * Creates TaskInfo objects to launch NMs as mesos tasks.
   */
  class NMTaskFactoryImpl implements TaskFactory {
    public static final String EXECUTOR_NAME = "myriad_task";
    public static final String EXECUTOR_PREFIX = "myriad_executor";
    public static final String YARN_NODEMANAGER_OPTS_KEY = "YARN_NODEMANAGER_OPTS";

    private static final Logger LOGGER = LoggerFactory.getLogger(NMTaskFactoryImpl.class);
    private static final Random rand = new Random();
    private MyriadConfiguration cfg;
    private TaskUtils taskUtils;
    private ExecutorCommandLineGenerator clGenerator;

    @Inject
    public NMTaskFactoryImpl(MyriadConfiguration cfg, TaskUtils taskUtils, ExecutorCommandLineGenerator clGenerator) {
      this.cfg = cfg;
      this.taskUtils = taskUtils;
      this.clGenerator = clGenerator;
    }

    @Override
    public TaskInfo createTask(Offer offer, FrameworkID frameworkId, TaskID taskId, NodeTask nodeTask) {
      Objects.requireNonNull(offer, "Offer should be non-null");
      Objects.requireNonNull(nodeTask, "NodeTask should be non-null");

      AbstractPorts ports = taskUtils.getRandomPortResources(offer, 4, new TreeSet<Long>());
      LOGGER.debug(ports.toString());

      ServiceResourceProfile serviceProfile = nodeTask.getProfile();
      Double taskMemory = serviceProfile.getAggregateMemory();
      Double taskCpus = serviceProfile.getAggregateCpu();

      CommandInfo commandInfo = clGenerator.generateCommandLine(serviceProfile, null, ports);
      ExecutorInfo executorInfo = getExecutorInfoForSlave(frameworkId, offer, commandInfo);

      TaskInfo.Builder taskBuilder = TaskInfo.newBuilder().setName(cfg.getFrameworkName() + "-" + taskId.getValue()).setTaskId(taskId).setSlaveId(
          offer.getSlaveId());

      return taskBuilder
          .addAllResources(taskUtils.getScalarResource(offer, "cpus", taskCpus, taskUtils.getExecutorCpus()))
          .addAllResources(taskUtils.getScalarResource(offer, "mem", taskMemory, taskUtils.getExecutorMemory()))
          .addAllResources(ports.createResourceList())
          .setExecutor(executorInfo)
          .build();
    }

    @Override
    public ExecutorInfo getExecutorInfoForSlave(FrameworkID frameworkId, Offer offer, CommandInfo commandInfo) {
      ExecutorID executorId = ExecutorID.newBuilder()
          .setValue(EXECUTOR_PREFIX + frameworkId.getValue() + offer.getId().getValue() + offer.getSlaveId().getValue())
          .build();
      ExecutorInfo.Builder executorInfo = ExecutorInfo.newBuilder().setCommand(commandInfo).setName(EXECUTOR_NAME).setExecutorId(executorId)
              .addAllResources(taskUtils.getScalarResource(offer, "cpus", taskUtils.getExecutorCpus(), 0.0))
              .addAllResources(taskUtils.getScalarResource(offer, "mem", taskUtils.getExecutorMemory(), 0.0));
      if (cfg.getContainerInfo().isPresent()) {
        executorInfo.setContainer(taskUtils.getContainerInfo());
      }
      return executorInfo.build();
    }
  }

  /**
   * Implement NM Task Constraints
   */
  public static class NMTaskConstraints implements TaskConstraints {
    @Override
    public int portsCount() {
      return NMPorts.expectedNumPorts();
    }
  }

}
