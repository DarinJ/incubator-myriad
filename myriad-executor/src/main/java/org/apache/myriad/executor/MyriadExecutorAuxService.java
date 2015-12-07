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

package org.apache.myriad.executor;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.google.common.collect.Sets;
import org.apache.hadoop.yarn.server.api.ApplicationInitializationContext;
import org.apache.hadoop.yarn.server.api.ApplicationTerminationContext;
import org.apache.hadoop.yarn.server.api.AuxiliaryService;
import org.apache.hadoop.yarn.server.api.ContainerInitializationContext;
import org.apache.hadoop.yarn.server.api.ContainerTerminationContext;
import org.apache.mesos.MesosExecutorDriver;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.Status;
import org.apache.mesos.Protos.TaskState;
import org.apache.mesos.Protos.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Auxillary service wrapper for MyriadExecutor
 */
public class MyriadExecutorAuxService extends AuxiliaryService {

  private static final Logger LOGGER = LoggerFactory.getLogger(MyriadExecutor.class);
  private static final String SERVICE_NAME = "myriad_service";
  public static final String YARN_CONTAINER_TASK_ID_PREFIX = "yarn_";

  private MesosExecutorDriver driver;
  private Thread myriadExecutorThread;

  // Storing application id and container id strings as it is difficult to get access to
  // NodeManager's NMContext object from an auxiliary service.
  private ConcurrentHashMap<String, Set<String>> applicationIds = new ConcurrentHashMap<String, Set<String>>();

  protected MyriadExecutorAuxService() {
    super(SERVICE_NAME);
  }

  @Override
  protected void serviceStart() throws Exception {

    LOGGER.info("Starting MyriadExecutor...");

    myriadExecutorThread = new Thread(new Runnable() {
      public void run() {
        driver = new MesosExecutorDriver(new MyriadExecutor(applicationIds));
        LOGGER.error("MyriadExecutor exit with status " + Integer.toString(driver.run() == Status.DRIVER_STOPPED ? 0 : 1));
      }
    });
    myriadExecutorThread.start();
  }

  @Override
  public void initializeApplication(ApplicationInitializationContext initAppContext) {
    Set<String> containers = Sets.newConcurrentHashSet();
    applicationIds.putIfAbsent(initAppContext.getApplicationId().toString(), containers);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("initializeApplication");
    }
  }

  @Override
  public void stopApplication(ApplicationTerminationContext stopAppContext) {
    String applicationId = stopAppContext.getApplicationId().toString();
    System.out.println(applicationId);
    Set<String> containers = applicationIds.remove((Object) applicationId);
    if (containers != null) {
      for (String container: containers) {
        sendStatus(container, TaskState.TASK_FINISHED);
      }
    }
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("stopApplication");
    }
  }

  @Override
  public ByteBuffer getMetaData() {
    LOGGER.debug("getMetaData");
    return null;
  }

  @Override
  public void initializeContainer(ContainerInitializationContext initContainerContext) {
    String containerId = initContainerContext.getContainerId().toString();
    String applicationId = initContainerContext.getContainerId().getApplicationAttemptId().getApplicationId().toString();
    applicationIds.get(applicationId).add(containerId);
    sendStatus(containerId, TaskState.TASK_RUNNING);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Received Container init request for appID" + applicationId);
    }
  }

  @Override
  public void stopContainer(ContainerTerminationContext stopContainerContext) {
    String containerId = stopContainerContext.getContainerId().toString();
    String applicationId = stopContainerContext.getContainerId().getApplicationAttemptId().getApplicationId().toString();
    applicationIds.get(applicationId).remove(containerId);
    sendStatus(containerId, TaskState.TASK_FINISHED);
    if (LOGGER.isDebugEnabled()) {
      LOGGER.debug("Received Container stop request for appID" + applicationId);
    }
  }

  private void sendStatus(String containerId, TaskState taskState) {
    Protos.TaskID taskId = Protos.TaskID.newBuilder().setValue(YARN_CONTAINER_TASK_ID_PREFIX + containerId).build();
    TaskStatus status = TaskStatus.newBuilder().setTaskId(taskId).setState(taskState).build();
    driver.sendStatusUpdate(status);
    LOGGER.debug("Sent status " + taskState + " for taskId " + taskId);
  }
}
