package org.apache.myriad.scheduler.offer;

import org.apache.mesos.Protos;

import java.util.Collections;


/**
 * Created by darin on 6/22/16.
 */
public class OfferBuilder {
  Protos.Offer.Builder offer = Protos.Offer.newBuilder();

  public OfferBuilder(String offerId, String hostname, String slaveId) {
    offer.setHostname(hostname);
    offer.setId(Protos.OfferID.newBuilder().setValue(offerId));
    offer.setSlaveId(Protos.SlaveID.newBuilder().setValue(slaveId));
    offer.setFrameworkId(Protos.FrameworkID.newBuilder().setValue("Myriad"));
  }
  public OfferBuilder(String offerId, String hostname) {
    offer.setHostname(hostname);
    offer.setId(Protos.OfferID.newBuilder().setValue(offerId));
    offer.setSlaveId(Protos.SlaveID.newBuilder().setValue("agent"));
    offer.setFrameworkId(Protos.FrameworkID.newBuilder().setValue("Myriad"));
  }
  public OfferBuilder(String hostname) {
    offer.setHostname(hostname);
    offer.setId(Protos.OfferID.newBuilder().setValue("test"));
    offer.setSlaveId(Protos.SlaveID.newBuilder().setValue("agent"));
    offer.setFrameworkId(Protos.FrameworkID.newBuilder().setValue("Myriad"));
  }

  public OfferBuilder addScalarResource(String name, double value) {
    offer.addAllResources(Collections.singleton(createScalarResource(name, value)));
    return this;
  }

  public OfferBuilder addScalarResource(String name, String role, double value) {
    offer.addAllResources(Collections.singleton(createScalarResource(name, role, value)));
    return this;
  }

  public OfferBuilder addRangeResource(String name, long begin, long end) {
    offer.addAllResources(Collections.singleton(createRangeResource(name, begin, end)));
    return this;
  }

  public OfferBuilder addRangeResource(String name, String role, long begin, long end) {
    offer.addAllResources(Collections.singleton(createRangeResource(name, role, begin, end)));
    return this;
  }

  public Protos.Offer build() {
    return offer.build();
  }

  private static Protos.Resource createScalarResource(String name, String role, double value) {
    return Protos.Resource.newBuilder()
        .setScalar(Protos.Value.Scalar.newBuilder().setValue(value))
        .setType(Protos.Value.Type.SCALAR)
        .setRole(role)
        .setName(name)
        .build();
  }

  private static Protos.Resource createScalarResource(String name, double value) {
    return Protos.Resource.newBuilder()
        .setScalar(Protos.Value.Scalar.newBuilder().setValue(value))
        .setType(Protos.Value.Type.SCALAR)
        .setName(name)
        .build();
  }

  private static Protos.Resource createRangeResource(String name, long begin, long end) {
    return Protos.Resource.newBuilder()
        .setName(name)
        .setType(Protos.Value.Type.RANGES)
        .setRanges(Protos.Value.Ranges.newBuilder()
            .addRange(Protos.Value.Range.newBuilder()
                .setBegin(begin)
                .setEnd(end)
                .build())
            .build())
        .build();
  }

  private static Protos.Resource createRangeResource(String name, String role, long begin, long end) {
    return Protos.Resource.newBuilder()
        .setName(name)
        .setType(Protos.Value.Type.RANGES)
        .setRole(role)
        .setRanges(Protos.Value.Ranges.newBuilder()
            .addRange(Protos.Value.Range.newBuilder()
                .setBegin(begin)
                .setEnd(end)
                .build())
            .build())
        .build();
  }
}
