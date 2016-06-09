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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.myriad.scheduler;

import java.util.*;
import javax.inject.Inject;

import com.google.common.collect.Lists;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.ExecutorInfo;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.myriad.configuration.MyriadConfiguration;
import org.apache.myriad.configuration.ServiceConfiguration;
import org.apache.myriad.state.NodeTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Generic Service Class that allows to create a service solely base don the configuration
 * Main properties of configuration are:
 * 1. command to run
 * 2. Additional env. variables to set (serviceOpts)
 * 3. ports to use with names of the properties
 * 4. TODO (yufeldman) executor info
 */
public class ServiceTaskFactoryImpl implements TaskFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceTaskFactoryImpl.class);

  public static final long DEFAULT_PORT_NUMBER = 0;

  private MyriadConfiguration cfg;
  @SuppressWarnings("unused")
  private TaskUtils taskUtils;
  private ServiceCommandLineGenerator clGenerator;

  @Inject
  public ServiceTaskFactoryImpl(MyriadConfiguration cfg, TaskUtils taskUtils) {
    this.cfg = cfg;
    this.taskUtils = taskUtils;
    this.clGenerator = new ServiceCommandLineGenerator(cfg);
  }
  
  @Override
  public TaskInfo createTask(Offer offer, FrameworkID frameworkId, TaskID taskId, NodeTask nodeTask) {
    Objects.requireNonNull(offer, "Offer should be non-null");
    Objects.requireNonNull(nodeTask, "NodeTask should be non-null");

    ServiceConfiguration serviceConfig = cfg.getServiceConfiguration(nodeTask.getTaskPrefix()).get();

    Objects.requireNonNull(serviceConfig, "ServiceConfig should be non-null");
    Objects.requireNonNull(serviceConfig.getCommand().orNull(), "command for ServiceConfig should be non-null");

    Map<String, Long> portsMap = serviceConfig.getPorts().or(new HashMap<String, Long>());
    AbstractPorts ports = taskUtils.getPortResources(offer, Lists.newArrayList(portsMap.values()), new HashSet());

    CommandInfo commandInfo = clGenerator.generateCommandLine(nodeTask.getProfile(), serviceConfig, ports);

    LOGGER.info("Command line for service: {} is: {}", commandInfo.getValue());

    TaskInfo.Builder taskBuilder = TaskInfo.newBuilder();

    taskBuilder.setName(nodeTask.getTaskPrefix()).setTaskId(taskId).setSlaveId(offer.getSlaveId())
        .addAllResources(taskUtils.getScalarResource(offer, "cpus", nodeTask.getProfile().getCpus(), 0.0))
        .addAllResources(taskUtils.getScalarResource(offer, "mem", nodeTask.getProfile().getMemory(), 0.0))
        .addAllResources(ports.createResourceList());

    //taskBuilder.addResources(Resource.newBuilder().setName("ports").setType(Value.Type.RANGES).setRanges(valueRanger.build()));
    taskBuilder.setCommand(commandInfo);
    if (cfg.getContainerInfo().isPresent()) {
      taskBuilder.setContainer(taskUtils.getContainerInfo());
    }
    return taskBuilder.build();
  }

  @Override
  public ExecutorInfo getExecutorInfoForSlave(FrameworkID frameworkId, Offer offer, CommandInfo commandInfo) {
    // TODO (yufeldman) if executor specified use it , otherwise return null
    // nothing to implement here, since we are using default slave executor
    return null;
  }

}
