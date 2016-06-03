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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.myriad.configuration.MyriadConfiguration;
import org.apache.myriad.configuration.ServiceConfiguration;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Class to test CommandLine generation
 */
public class TestServiceCommandLine {

  static MyriadConfiguration cfg;

  private static final String msgFormat = System.lineSeparator() + "%s" + System.lineSeparator() + "!="
      + System.lineSeparator() + "%s";
  protected static final String CMD_FORMAT = "echo \"%1$s\" && %1$s";
  static String toJHSCompare =
      " sudo tar -zxpf hadoop-2.7.0.tar.gz &&  sudo  cp conf /usr/local/hadoop/etc/hadoop/yarn-site.xml &&  " +
          "sudo -E -u hduser -H  bin/mapred historyserver";
  static String toCompare =
      " sudo tar -zxpf hadoop-2.7.0.tar.gz &&  sudo  cp conf /usr/local/hadoop/etc/hadoop/yarn-site.xml &&  " +
          "sudo -E -u hduser -H  $YARN_HOME/bin/yarn nodemanager";

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
    cfg = mapper.readValue(Thread.currentThread().getContextClassLoader().getResource("myriad-config-test-default.yml"),
        MyriadConfiguration.class);

  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
  }

  @Test
  public void testJHSCommandLineGeneration() throws Exception {
    String executorCmd = "$YARN_HOME/bin/mapred historyserver";
    ServiceResourceProfile profile = new ServiceResourceProfile("jobhistory", 10.0, 15.0);
    ServiceConfiguration serviceConfiguration = cfg.getServiceConfiguration("jobhistory");
    ServiceCommandLineGenerator serviceCommandLineGenerator = new ServiceCommandLineGenerator(cfg);
    AbstractPorts ports = new AbstractPorts();
    ports.add(1L);
    ports.add(2L);
    ports.add(3L);
    CommandInfo cInfo = serviceCommandLineGenerator.generateCommandLine(profile,
        serviceConfiguration,
        ports);
    String testVal =  String.format(CMD_FORMAT, toJHSCompare);
    assertTrue(String.format(msgFormat, cInfo.getValue(), testVal),
        cInfo.getValue().equals(testVal));
  }

  @Test
  public void testNMCommandLineGeneration() throws Exception {
    Long[] ports = new Long[]{1L, 2L, 3L, 4L};
    AbstractPorts nmPorts = new AbstractPorts();
    for (Long port : ports) {
      nmPorts.add(port);
    }

    ServiceResourceProfile profile = new ExtendedResourceProfile(new NMProfile("nm", 10L, 15L), 3.0, 5.0);

    ExecutorCommandLineGenerator clGenerator = new NMExecutorCLGenImpl(cfg);

    CommandInfo cInfo = clGenerator.generateCommandLine(profile, nmPorts);
    String testVal =  String.format(CMD_FORMAT, toCompare);
    System.out.println();
    System.out.println(toCompare);
    System.out.println(testVal);
    System.out.println(cInfo.getValue());
    assertTrue(String.format(msgFormat, cInfo.getValue(), testVal),
        cInfo.getValue().equals(testVal));
  }
}
