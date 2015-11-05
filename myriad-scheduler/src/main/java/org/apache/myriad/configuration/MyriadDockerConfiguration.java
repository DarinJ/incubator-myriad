package org.apache.myriad.configuration;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

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

  public String getImage() {
    return image;
  }

  public String getNetwork() {
    return (network != null) ? network : "BRIDGED";
  }

  public Boolean getPrivledged() {
    return privledged != null ? privledged : false;
  }

}
