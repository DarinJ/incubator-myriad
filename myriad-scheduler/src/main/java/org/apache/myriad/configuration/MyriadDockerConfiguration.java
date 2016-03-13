package org.apache.myriad.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by darinj on 11/4/15.
 */
public class MyriadDockerConfiguration {
  @JsonProperty
  @NotEmpty
  String image;

  @JsonProperty
  String network;

  @JsonProperty
  Boolean privledged;

  @JsonProperty
  List<Map<String, String>> parameters;
  @JsonProperty

  @NotEmpty
  public String getImage() {
    return image;
  }

  public String getNetwork() {
    return (network != null) ? network : "HOST";
  }

  public Boolean getPrivledged() {
    return privledged != null ? privledged : false;
  }

  public Iterable<Map<String, String>> getParameters() {
    if (parameters == null) {
      return new ArrayList<>();
    } else {
      return parameters;
    }
  }
}
