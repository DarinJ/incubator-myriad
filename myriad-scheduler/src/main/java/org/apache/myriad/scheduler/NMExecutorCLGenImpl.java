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

import java.util.HashMap;
import java.util.Map;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.mesos.Protos;
import org.apache.mesos.Protos.CommandInfo;
import org.apache.myriad.configuration.MyriadConfiguration;
import org.apache.myriad.configuration.MyriadExecutorConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Implementation assumes NM binaries already deployed
 */
public class NMExecutorCLGenImpl implements ExecutorCommandLineGenerator {

  private static final Logger LOGGER = LoggerFactory.getLogger(NMExecutorCLGenImpl.class);
  protected static final String CMD_FORMAT = "echo \"%1$s\" && %1$s";


  public static final String ENV_YARN_NODEMANAGER_OPTS = "YARN_NODEMANAGER_OPTS";
  public static final String KEY_YARN_NM_CGROUPS_PATH = "yarn.nodemanager.cgroups.path";
  public static final String KEY_YARN_RM_HOSTNAME = "yarn.resourcemanager.hostname";

  /**
   * YARN class to help handle LCE resources
   */
  public static final String KEY_YARN_NM_LCE_CGROUPS_HIERARCHY = "yarn.nodemanager.linux-container-executor.cgroups.hierarchy";
  public static final String KEY_YARN_HOME = "yarn.home";
  public static final String KEY_NM_RESOURCE_CPU_VCORES = "nodemanager.resource.cpu-vcores";
  public static final String KEY_NM_RESOURCE_MEM_MB = "nodemanager.resource.memory-mb";
  public static final String YARN_NM_CMD = " $YARN_HOME/bin/yarn nodemanager";
  public static final String KEY_NM_ADDRESS = "myriad.yarn.nodemanager.address";
  public static final String KEY_NM_LOCALIZER_ADDRESS = "myriad.yarn.nodemanager.localizer.address";
  public static final String KEY_NM_WEBAPP_ADDRESS = "myriad.yarn.nodemanager.webapp.address";
  public static final String KEY_NM_SHUFFLE_PORT = "myriad.mapreduce.shuffle.port";

  protected static final String ALL_LOCAL_IPV4ADDR = "0.0.0.0:";
  protected static final String PROPERTY_FORMAT = " -D%s=%s ";
  protected CommandInfo staticCommandInfo;
  protected MyriadConfiguration cfg;
  protected YarnConfiguration conf = new YarnConfiguration();
  protected MyriadExecutorConfiguration myriadExecutorConfiguration;
  protected NMExecutorCLGenImpl() {
    //never call this only for subclasses
  }

  public NMExecutorCLGenImpl(MyriadConfiguration cfg) {
    this.cfg = cfg;
    this.myriadExecutorConfiguration = cfg.getMyriadExecutorConfiguration();
    generateStaticCommandLine();
  }

  @Override
  public Protos.CommandInfo generateCommandLine(ServiceResourceProfile profile, AbstractPorts ports) {
    CommandInfo.Builder builder = CommandInfo.newBuilder();
    builder.mergeFrom(staticCommandInfo);
    builder.setEnvironment(generateEnvironment(profile, ports));
    builder.setUser(getUser());
    return builder.build();
  }

  protected void generateStaticCommandLine() {
    CommandInfo.Builder builder = CommandInfo.newBuilder();
    StringBuilder cmdLine = new StringBuilder();
    appendCgroupsCmds(cmdLine);
    appendDistroExtractionCommands(cmdLine);
    appendUserSudo(cmdLine);
    cmdLine.append(YARN_NM_CMD);
    builder.setValue(String.format(CMD_FORMAT, cmdLine.toString()));
    builder.addAllUris(getUris());
    staticCommandInfo = builder.build();
  }

  protected String getUser() {
    if (cfg.getFrameworkSuperUser().isPresent()) {
      return cfg.getFrameworkSuperUser().get();
    } else {
      return cfg.getFrameworkUser().get();
    }
  }

  protected List<CommandInfo.URI> getUris() {
    List<CommandInfo.URI> uris = new ArrayList<>();
    if (myriadExecutorConfiguration.getJvmUri().isPresent()) {
      final String jvmRemoteUri = myriadExecutorConfiguration.getJvmUri().get();
      LOGGER.info("Getting JRE distribution from:" + jvmRemoteUri);
      uris.add(CommandInfo.URI.newBuilder().setValue(jvmRemoteUri).build());
    }
    if (myriadExecutorConfiguration.getConfigUri().isPresent()) {
      String configURI = myriadExecutorConfiguration.getConfigUri().get();
      LOGGER.info("Getting Hadoop distribution from: {}", configURI);
      uris.add(CommandInfo.URI.newBuilder().setValue(configURI).build());
    }
    if (myriadExecutorConfiguration.getNodeManagerUri().isPresent()) {
      //Both FrameworkUser and FrameworkSuperuser to get all of the directory permissions correct.
      if (!(cfg.getFrameworkUser().isPresent() && cfg.getFrameworkSuperUser().isPresent())) {
        LOGGER.warn("Trying to use remote distribution, but frameworkUser and/or frameworkSuperUser not set!" +
            "Some features may not work");
      }
      String nodeManagerUri = myriadExecutorConfiguration.getNodeManagerUri().get();
      LOGGER.info("Getting Hadoop distribution from: {}", nodeManagerUri);
      uris.add(CommandInfo.URI.newBuilder().setValue(nodeManagerUri).setExtract(false).build());
    }
    return uris;
  }

  protected Protos.Environment generateEnvironment(ServiceResourceProfile profile, AbstractPorts ports) {
    Map<String, String> yarnEnv = cfg.getYarnEnvironment();
    Protos.Environment.Builder builder = Protos.Environment.newBuilder();
    builder.addAllVariables(Iterables.transform(yarnEnv.entrySet(), new Function<Map.Entry<String, String>, Protos.Environment.Variable>() {
      public Protos.Environment.Variable apply(Map.Entry<String, String> x) {
        return Protos.Environment.Variable.newBuilder().setName(x.getKey()).setValue(x.getValue()).build();
      }
    }));

    StringBuilder yarnOpts = new StringBuilder();
    String rmHostName = System.getProperty(KEY_YARN_RM_HOSTNAME);
    if (StringUtils.isNotEmpty(rmHostName)) {
      addYarnNodemanagerOpt(KEY_YARN_RM_HOSTNAME, rmHostName);

    if (StringUtils.isNoneEmpty(rmHostName)) {
      addYarnNodemanagerOpt(yarnOpts, KEY_YARN_RM_HOSTNAME, rmHostName);
    }

    if (cfg.getNodeManagerConfiguration().getCgroups()) {
      addYarnNodemanagerOpt(KEY_YARN_NM_LCE_CGROUPS_HIERARCHY, "mesos/$TASK_DIR");
      if (environment.containsKey("YARN_HOME")) {
        addYarnNodemanagerOpt(KEY_YARN_HOME, environment.get("YARN_HOME"));
      }
    if (yarnEnv.containsKey(KEY_YARN_HOME)) {
      addYarnNodemanagerOpt(yarnOpts, KEY_YARN_HOME, yarnEnv.get("YARN_HOME"));
    }

    addYarnNodemanagerOpt(yarnOpts, KEY_YARN_NM_LCE_CGROUPS_HIERARCHY, "mesos/$TASK_DIR");
    addYarnNodemanagerOpt(yarnOpts, KEY_NM_RESOURCE_CPU_VCORES, Integer.toString(profile.getCpus().intValue()));
    addYarnNodemanagerOpt(yarnOpts, KEY_NM_RESOURCE_MEM_MB, Integer.toString(profile.getMemory().intValue()));
    addYarnNodemanagerOpt(yarnOpts, KEY_NM_ADDRESS, ALL_LOCAL_IPV4ADDR + Long.toString(ports.get(0).getPort()));
    addYarnNodemanagerOpt(yarnOpts, KEY_NM_LOCALIZER_ADDRESS, ALL_LOCAL_IPV4ADDR + Long.toString(ports.get(1).getPort()));
    addYarnNodemanagerOpt(yarnOpts, KEY_NM_WEBAPP_ADDRESS, ALL_LOCAL_IPV4ADDR + Long.toString(ports.get(2).getPort()));
    addYarnNodemanagerOpt(yarnOpts, KEY_NM_SHUFFLE_PORT, Long.toString(ports.get(3).getPort()));

    if (cfg.getYarnEnvironment().containsKey(ENV_YARN_NODEMANAGER_OPTS)) {
      yarnOpts.append(" ").append(yarnEnv.get(ENV_YARN_NODEMANAGER_OPTS));
    }
    builder.addAllVariables(Collections.singleton(
            Protos.Environment.Variable.newBuilder()
                .setName(ENV_YARN_NODEMANAGER_OPTS)
                .setValue(yarnOpts.toString()).build())
    );
    return builder.build();
  }

  protected void addYarnNodemanagerOpt(StringBuilder opts, String propertyName, String propertyValue) {
    String envOpt = String.format(PROPERTY_FORMAT, propertyName, propertyValue);
    opts.append(envOpt);
  }

  protected void appendCgroupsCmds(StringBuilder cmdLine) {
    //These can't be set in the environment as they require commands to be run on the host
    if (cfg.getFrameworkSuperUser().isPresent() && cfg.isCgroupEnabled()) {
      cmdLine.append(" export TASK_DIR=`basename $PWD`&&");
      //The container executor script expects mount-path to exist and owned by the yarn user
      //See: https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/NodeManagerCgroups.html
      //If YARN ever moves to cgroup/mem it will be necessary to add a mem version.
      appendSudo(cmdLine);
      cmdLine.append("chown " + cfg.getFrameworkUser().get() + " ");
      cmdLine.append(cfg.getCGroupPath());
      cmdLine.append("/cpu/mesos/$TASK_DIR &&");
      cmdLine.append(String.format("export %s=%s -D%s=%s", ENV_YARN_NODEMANAGER_OPTS, ENV_YARN_NODEMANAGER_OPTS,
          KEY_YARN_NM_LCE_CGROUPS_HIERARCHY, "mesos/$TASK_DIR"));
    } else if (cfg.isCgroupEnabled()) {
      LOGGER.info("frameworkSuperUser not set ignoring cgroup configuration, this will likely can the nodemanager to crash");
    }
  }

  protected void appendSudo(StringBuilder cmdLine) {
    if (cfg.getFrameworkSuperUser().isPresent()) {
      cmdLine.append(" sudo ");
    }
  }

  protected void appendUserSudo(StringBuilder cmdLine) {
    if (cfg.getFrameworkSuperUser().isPresent()) {
      cmdLine.append(" sudo -E -u ");
      cmdLine.append(cfg.getFrameworkUser().get());
      cmdLine.append(" -H ");
    }
  }

  @Override
  public String getConfigurationUrl() {
    String httpPolicy = conf.get(TaskFactory.YARN_HTTP_POLICY);
    String address = StringUtils.EMPTY;
    if (httpPolicy != null && httpPolicy.equals(TaskFactory.YARN_HTTP_POLICY_HTTPS_ONLY)) {
      address = conf.get(TaskFactory.YARN_RESOURCEMANAGER_WEBAPP_HTTPS_ADDRESS);
      if (StringUtils.isEmpty(address)) {
        address = conf.get(TaskFactory.YARN_RESOURCEMANAGER_HOSTNAME) + ":8090";
      }
    } else {
      address = conf.get(TaskFactory.YARN_RESOURCEMANAGER_WEBAPP_ADDRESS);
      if (StringUtils.isEmpty(address)) {
        address = conf.get(TaskFactory.YARN_RESOURCEMANAGER_HOSTNAME) + ":8088";
      }
    }

    return "http://" + address + "/conf";
  }

  protected void appendDistroExtractionCommands(StringBuilder cmdLine) {
    /*
    TODO(darinj): Overall this is messier than I'd like. We can't let mesos untar the distribution, since
    it will change the permissions.  Instead we simply download the tarball and execute tar -xvpf. We also
    pull the config from the resource manager and put them in the conf dir.  This is also why we need
    frameworkSuperUser. This will be refactored after Mesos-1790 is resolved.
   */

    //TODO(DarinJ) support other compression, as this is a temp fix for Mesos 1760 may not get to it.
    if (myriadExecutorConfiguration.getNodeManagerUri().isPresent()) {
      //Extract tarball keeping permissions, necessary to keep HADOOP_HOME/bin/container-executor suidbit set.
      //If SudoUser not enable LinuxExecutor will not work
      appendSudo(cmdLine);
      cmdLine.append("tar -zxpf ").append(getFileName(myriadExecutorConfiguration.getNodeManagerUri().get()));
      cmdLine.append(" && ");
      //Place the hadoop config where in the HADOOP_CONF_DIR where it will be read by the NodeManager
      //The url for the resource manager config is: http(s)://hostname:port/conf so fetcher.cpp downloads the
      //config file to conf, It's an xml file with the parameters of yarn-site.xml, core-site.xml and hdfs.xml.
      if (!myriadExecutorConfiguration.getConfigUri().isPresent()) {
        appendSudo(cmdLine);
        cmdLine.append(" cp conf ");
        cmdLine.append(cfg.getYarnEnvironment().get("YARN_HOME"));
        cmdLine.append("/etc/hadoop/yarn-site.xml && ");
      }
    }
  }

  private static String getFileName(String uri) {
    int lastSlash = uri.lastIndexOf('/');
    if (lastSlash == -1) {
      return uri;
    } else {
      String fileName = uri.substring(lastSlash + 1);
      Preconditions.checkArgument(!Strings.isNullOrEmpty(fileName), "URI should not have a slash at the end");
      return fileName;
    }
  }
}
