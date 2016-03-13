package org.apache.myriad.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.apache.mesos.Protos;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by darinj on 11/4/15.
 */
public class MyriadContainerConfiguration {
  @JsonProperty
  @NotEmpty
  private String type;
  @JsonProperty
  private MyriadDockerConfiguration dockerConfiguration;
  @JsonProperty
  private List<Map<String, String>> volumes;

  public Protos.ContainerInfo.Type getType() throws ClassNotFoundException, IllegalAccessException, InstantiationException {
    if (type != null) {
      return (Protos.ContainerInfo.Type) Class.forName("Protos.ContainerInfo.Type" + type).newInstance();
    } else {
      return Protos.ContainerInfo.Type.DOCKER;
    }
  }

  public Optional<MyriadDockerConfiguration> getDockerConfiguration() {
    return Optional.fromNullable(dockerConfiguration);
  }

  public Iterable<Map<String, String>> getVolumes() {
    return volumes == null ? new ArrayList<Map<String, String>>() : volumes;
  }
}
