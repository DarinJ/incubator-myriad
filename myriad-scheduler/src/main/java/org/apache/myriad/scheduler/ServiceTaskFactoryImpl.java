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

import com.google.common.annotations.VisibleForTesting;

import java.util.*;
import javax.inject.Inject;

import com.google.common.collect.Lists;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.mesos.Protos.CommandInfo.URI;
import org.apache.mesos.Protos.FrameworkID;
import org.apache.mesos.Protos.Offer;
import org.apache.mesos.Protos.TaskID;
import org.apache.mesos.Protos.TaskInfo;
import org.apache.myriad.configuration.MyriadConfiguration;
import org.apache.myriad.configuration.MyriadExecutorConfiguration;
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
    this.clGenerator = new ServiceCommandLineGenerator(cfg, cfg.getMyriadExecutorConfiguration().getNodeManagerUri().orNull());
  }

  @Override
  public TaskInfo createTask(Offer offer, FrameworkID frameworkId, TaskID taskId, NodeTask nodeTask) {
    Objects.requireNonNull(offer, "Offer should be non-null");
    Objects.requireNonNull(nodeTask, "NodeTask should be non-null");

    ServiceConfiguration serviceConfig = cfg.getServiceConfiguration(nodeTask.getTaskPrefix());

    Objects.requireNonNull(serviceConfig, "ServiceConfig should be non-null");
    Objects.requireNonNull(serviceConfig.getCommand().orNull(), "command for ServiceConfig should be non-null");

    final String serviceHostName = "0.0.0.0";
    final String serviceEnv = serviceConfig.getEnvSettings();
    final String rmHostName = System.getProperty(YARN_RESOURCEMANAGER_HOSTNAME);
    List<Long> additionalPortsNumbers = null;


    final StringBuilder strB = new StringBuilder("env ");
    if (serviceConfig.getServiceOpts() != null) {
      strB.append(serviceConfig.getServiceOpts()).append("=");

      strB.append("\"");
      if (rmHostName != null && !rmHostName.isEmpty()) {
        strB.append("-D" + YARN_RESOURCEMANAGER_HOSTNAME + "=" + rmHostName + " ");
      }

      Map<String, Long> portsMap = serviceConfig.getPorts().or(new HashMap<String, Long>());
      AbstractPorts ports = taskUtils.getPortResources(offer, Lists.newArrayList(portsMap.values()), new HashSet<Long>());
      int i = 0;
      for (String portProperty : portsMap.keySet()) {
        strB.append("-D" + portProperty + "=" + serviceHostName + ":" + ports.get(i) + " ");
        i++;
      }
      strB.append(serviceEnv);
      strB.append("\"");
    }

    strB.append(" ");
    strB.append(serviceConfig.getCommand().get());

    CommandInfo commandInfo = createCommandInfo(nodeTask.getProfile(), strB.toString());

    LOGGER.info("Command line for service: {} is: {}", nodeTask.getTaskPrefix(), strB.toString());

    TaskInfo.Builder taskBuilder = TaskInfo.newBuilder();

    taskBuilder.setName(nodeTask.getTaskPrefix()).setTaskId(taskId).setSlaveId(offer.getSlaveId())
        .addAllResources(taskUtils.getScalarResource(offer, "cpus", nodeTask.getProfile().getCpus(), 0.0))
        .addAllResources(taskUtils.getScalarResource(offer, "mem", nodeTask.getProfile().getMemory(), 0.0));
    taskBuilder.setExecutor(taskUtils.getExecutorInfoForSlave(frameworkId, offer, commandInfo));
    //taskBuilder.setCommand(commandInfo);
    return taskBuilder.build();
  }

  @VisibleForTesting
  CommandInfo createCommandInfo(ServiceResourceProfile profile, String executorCmd) {
    MyriadExecutorConfiguration myriadExecutorConfiguration = cfg.getMyriadExecutorConfiguration();
    CommandInfo.Builder commandInfo = CommandInfo.newBuilder();
    Map<String, String> envVars = cfg.getYarnEnvironment();
    if (envVars != null && !envVars.isEmpty()) {
      Protos.Environment.Builder yarnHomeB = Protos.Environment.newBuilder();
      for (Map.Entry<String, String> envEntry : envVars.entrySet()) {
        Protos.Environment.Variable.Builder yarnEnvB = Protos.Environment.Variable.newBuilder();
        yarnEnvB.setName(envEntry.getKey()).setValue(envEntry.getValue());
        yarnHomeB.addVariables(yarnEnvB.build());
      }
      commandInfo.mergeEnvironment(yarnHomeB.build());
    }

    if (myriadExecutorConfiguration.getNodeManagerUri().isPresent()) {
      //Both FrameworkUser and FrameworkSuperuser to get all of the directory permissions correct.
      if (!(cfg.getFrameworkUser().isPresent() && cfg.getFrameworkSuperUser().isPresent())) {
        throw new RuntimeException("Trying to use remote distribution, but frameworkUser" + "and/or frameworkSuperUser not set!");
      }

      LOGGER.info("Using remote distribution");
      String clGeneratedCommand = clGenerator.generateCommandLine(profile, null);

      String nmURIString = myriadExecutorConfiguration.getNodeManagerUri().get();

      //Concatenate all the subcommands
      String cmd = clGeneratedCommand + " " + executorCmd;

      //get the nodemanagerURI
      //We're going to extract ourselves, so setExtract is false
      LOGGER.info("Getting Hadoop distribution from:" + nmURIString);
      URI nmUri = URI.newBuilder().setValue(nmURIString).setExtract(false).build();

      //get configs directly from resource manager
      String configUrlString = clGenerator.getConfigurationUrl();
      LOGGER.info("Getting config from:" + configUrlString);
      URI configUri = URI.newBuilder().setValue(configUrlString).build();

      LOGGER.info("Slave will execute command:" + cmd);
      commandInfo.addUris(nmUri).addUris(configUri).setValue("echo \"" + cmd + "\";" + cmd);
      commandInfo.setUser(cfg.getFrameworkSuperUser().get());

    } else {
      commandInfo.setValue(executorCmd);
    }
    return commandInfo.build();
  }

}
