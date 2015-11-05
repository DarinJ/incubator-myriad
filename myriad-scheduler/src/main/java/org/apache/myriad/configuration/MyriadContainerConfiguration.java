package org.apache.myriad.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.ArrayList;
import java.util.List;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.google.common.base.Optional;
import com.google.common.collect.Maps;
import org.hibernate.validator.constraints.NotEmpty;

import javax.annotation.Nonnull;
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

  @JsonProperty
  public String getType() {
    return type;
  }

  public Optional<MyriadDockerConfiguration> getDockerConfiguration() {
    return Optional.fromNullable(dockerConfiguration);
  }

  public Iterable<Map<String, String>> getVolumes() {
    return volumes == null ? new ArrayList<Map<String, String>>() : volumes;
  private Map<String,String> volumeMap;
  public String getType() {
    return type;
  }
  public Optional<MyriadDockerConfiguration> getDockerConfiguration() {
    return Optional.fromNullable(dockerConfiguration);
  }
  public Map<String,String> getVolumMap() {
    if(volumeMap==null)
      return Maps.newHashMap();
    else
      return volumeMap;
  }
}
