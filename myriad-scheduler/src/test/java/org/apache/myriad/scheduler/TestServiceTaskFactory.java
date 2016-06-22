package org.apache.myriad.scheduler;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.mesos.Protos;
import org.apache.myriad.configuration.MyriadConfiguration;
import org.apache.myriad.configuration.ServiceConfiguration;
import org.apache.myriad.scheduler.offer.OfferBuilder;
import org.apache.myriad.scheduler.resource.ResourceOfferContainer;
import org.apache.myriad.state.NodeTask;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;


/**
 * Created by darin on 6/22/16.
 */
public class TestServiceTaskFactory {
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
  public void testServiceTaskFactory() {
    ServiceCommandLineGenerator clGenerator = new ServiceCommandLineGenerator(cfgWithDocker);
    TaskUtils taskUtils = new TaskUtils(cfgWithDocker);
    Protos.Offer offer = new OfferBuilder("test.com")
        .addScalarResource("cpus", 10.0)
        .addScalarResource("mem", 16000)
        .addRangeResource("ports", 3500, 3600)
        .build();
    Map<String, ServiceConfiguration> stringServiceConfigurationMap = cfgWithDocker.getServiceConfigurations();
    System.out.print(stringServiceConfigurationMap);
    ServiceConfiguration serviceConfiguration = cfgWithDocker.getServiceConfigurations().get("jobhistory");
    ServiceResourceProfile profile = new ServiceResourceProfile("jobhistory", serviceConfiguration.getCpus(),
        serviceConfiguration.getJvmMaxMemoryMB(), serviceConfiguration.getPorts());
    NodeTask nodeTask = new NodeTask(profile, null);
    nodeTask.setTaskPrefix("jobhistory");
    ResourceOfferContainer roc = new ResourceOfferContainer(offer, profile);
    ServiceTaskFactory taskFactory = new ServiceTaskFactory(cfgWithDocker, taskUtils, clGenerator);
    Protos.TaskInfo taskInfo = taskFactory.createTask(roc, frameworkId, makeTaskId("nm.zero"), nodeTask);
    assertTrue("taskInfo should have a container", taskInfo.hasContainer());
    assertFalse("The container should not have an executor", taskInfo.hasExecutor());
    Protos.ContainerInfo containerInfo = taskInfo.getContainer();
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
