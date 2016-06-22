package org.apache.myriad.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.mesos.Protos;
import org.apache.myriad.configuration.MyriadConfiguration;
import org.apache.myriad.scheduler.offer.OfferBuilder;
import org.apache.myriad.scheduler.resource.ResourceOfferContainer;
import org.apache.myriad.state.NodeTask;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Created by darin on 6/22/16.
 */
public class TestNMTaskFactory {
  static MyriadConfiguration cfg;
  static MyriadConfiguration cfgWithRole;
  static MyriadConfiguration cfgWithDocker;
  static double epsilon = .0001;
  static Protos.FrameworkID frameworkId;
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    cfg = mapper.readValue(Thread.currentThread().getContextClassLoader().getResource("myriad-config-test-default.yml"),
        MyriadConfiguration.class);
    cfgWithRole = mapper.readValue(Thread.currentThread().getContextClassLoader().getResource("myriad-config-test-default-with-framework-role.yml"),
        MyriadConfiguration.class);
    cfgWithDocker = mapper.readValue(Thread.currentThread().getContextClassLoader().getResource("myriad-config-test-default-with-docker-info.yml"),
        MyriadConfiguration.class);
    frameworkId = Protos.FrameworkID.newBuilder().setValue(cfg.getFrameworkName()).build();
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Test
  public void testNMTaskFactory() {
    NMExecutorCommandLineGenerator clGenerator = new NMExecutorCommandLineGenerator(cfgWithDocker);
    TaskUtils taskUtils = new TaskUtils(cfgWithDocker);
    Protos.Offer offer = new OfferBuilder("test.com")
        .addScalarResource("cpus", 10.0)
        .addScalarResource("mem", 16000)
        .addRangeResource("ports", 3500, 3600)
        .build();
    ServiceResourceProfile profile = new ExtendedResourceProfile(new NMProfile("tooMuchCpu", 7L, 8000L), taskUtils.getNodeManagerCpus(),
        taskUtils.getNodeManagerMemory(), taskUtils.getNodeManagerPorts());
    NodeTask nodeTask = new NodeTask(profile, null);
    ResourceOfferContainer roc = new ResourceOfferContainer(offer, profile);
    NMTaskFactory taskFactory = new NMTaskFactory(cfgWithDocker, taskUtils, clGenerator);
    Protos.TaskInfo taskInfo = taskFactory.createTask(roc, frameworkId, makeTaskId("nm.zero"), nodeTask);
    assertFalse("taskInfo should not have a container", taskInfo.hasContainer());
    assertTrue("The container should have an executor", taskInfo.hasExecutor());
    Protos.ExecutorInfo executorInfo = taskInfo.getExecutor();
    assertTrue("executorInfo should have container", executorInfo.hasContainer());
    Protos.ContainerInfo containerInfo = executorInfo.getContainer();
    assertTrue("There should be two volumes", containerInfo.getVolumesCount() == 2);
    assertTrue("The first volume should be read only", containerInfo.getVolumes(0).getMode().equals(Protos.Volume.Mode.RO));
    assertTrue("The first volume should be read write", containerInfo.getVolumes(1).getMode().equals(Protos.Volume.Mode.RW));
    assertTrue("There should be a docker image", containerInfo.getDocker().hasImage());
    assertTrue("The docker image should be mesos/myraid", containerInfo.getDocker().getImage().equals("mesos/myriad"));
    assertTrue("Should be using host networking", containerInfo.getDocker().getNetwork().equals(Protos.ContainerInfo.DockerInfo.Network.HOST));
    assertTrue("There should be two parameters", containerInfo.getDocker().getParametersList().size() == 2);
    assertTrue("Privledged mode should be false", containerInfo.getDocker().getPrivileged() == false);
  }

  private Protos.TaskID makeTaskId(String taskId) {
    return Protos.TaskID.newBuilder().setValue(taskId).build();
  }
}
