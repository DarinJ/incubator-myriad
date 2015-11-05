package org.apache.myriad.configuration;
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
