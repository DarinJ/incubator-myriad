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
package org.apache.myriad.scheduler.resource;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import org.apache.mesos.Protos;

import java.util.*;

/**
 * Mutable POJO for handling RangeResources, specifically ports.
 */

public class RangeResource {
  private String name;
  private List<Long> resourceValues;
  @VisibleForTesting //This way we can set a seed to get deterministic values
  private Random random = new Random();

  public RangeResource(Protos.Offer offer, String name, Collection<Long> valuesRequested) {
    this.name = name;
    setRangeResource(offer, valuesRequested);
  }

  public boolean satisfies(Collection<Long> requestedValues) {
    List<Long> tmp = new ArrayList<>();
    tmp.addAll(requestedValues);
    tmp.removeAll(resourceValues);
    tmp.removeAll(Collections.singleton(0L));
    return tmp.isEmpty();
  }

  public List<Long> getValues() {
    List<Long> ret = new ArrayList<>();
    ret.addAll(resourceValues);
    return ret;
  }

  public List<Protos.Resource> consumeResource(Collection<Long> requestedValues) {
    Preconditions.checkState(satisfies(requestedValues));
    List<Protos.Resource> resources = new ArrayList<>();
    for (Long value : requestedValues) {
      resources.add(createResource(value));
    }
    resourceValues.removeAll(requestedValues);
    return resources;
  }

  private Protos.Resource createResource(Long value) {
    return Protos.Resource.newBuilder()
        .setName(name)
        .setType(Protos.Value.Type.RANGES)
        .setRanges(Protos.Value.Ranges.newBuilder()
            .addRange(Protos.Value.Range.newBuilder()
                .setBegin(value)
                .setEnd(value)
                .build()
            )
        ).build();
  }

  private List<Long> getNonZeroes(Collection<Long> values) {
    List<Long> nonZeros = new ArrayList<>();
    for (Long value : values) {
      if (!value.equals(0L)) {
        nonZeros.add(value);
      }
    }
    return nonZeros;
  }

  private boolean allNonZeroesPresent(Protos.Offer offer, Collection<Long> values) {
    List<Long> nonZeros = getNonZeroes(values);
    int numFound = 0;
    for (Protos.Resource resource : offer.getResourcesList()) {
      if (resource.hasRanges() && resource.getName().equals(name)) {
        for (Protos.Value.Range range : resource.getRanges().getRangeList()) {
          Long begin = range.getBegin();
          Long end = range.getEnd();
          for (Long val:nonZeros) {
            if (val.longValue() >= begin && val.longValue() <= end) {
              numFound++;
            }
          }
        }
      }
    }
    return numFound == nonZeros.size();
  }

  private List<Long> getAllAvailableValues(Protos.Offer offer, Collection<Long> used) {
    List<Long> allValues = new ArrayList<>();
    for (Protos.Resource resource : offer.getResourcesList()) {
      if (resource.hasRanges() && resource.getName().equals(name) &&
          (!resource.hasRole() || resource.getRole().equals("*"))) {
        for (Protos.Value.Range range : resource.getRanges().getRangeList()) {
          Long begin = range.getBegin();
          Long end = range.getEnd();
          allValues.addAll(contiguousRange(begin, end));
        }
      }
    }
    allValues.removeAll(used);
    return allValues;
  }

  private List<Long> getRandomValues(List<Long> sampleSpace, int size) {
    List<Long> sample = new ArrayList<>(size);
    for (int i = 0; i < size; i++) {
      sample.add(sampleSpace.remove(random.nextInt(sampleSpace.size())));
    }
    return sample;
  }

  private List<Long> mergeRangeValues(Collection<Long> requested, Collection<Long> randomValues) {
    List<Long> values = new ArrayList<>();
    Iterator<Long> itr = randomValues.iterator();
    for (Long val : requested) {
      if (val == 0) {
        values.add(itr.next());
      } else {
        values.add(val);
      }
    }
    return values;
  }

  private void setRangeResource(Protos.Offer offer, Collection<Long> valuesRequested) {
    List<Long> resources = new ArrayList<>();
    List<Long> nonZeros = getNonZeroes(valuesRequested);
    //Make sure we get all requested ports otherwise return empty list
    if (!allNonZeroesPresent(offer, valuesRequested)) {
      resourceValues = resources;
      return;
    }
    List<Long> randomValues = getRandomValues(getAllAvailableValues(offer, valuesRequested), valuesRequested.size() - nonZeros.size());
    if (randomValues.size() + nonZeros.size() < valuesRequested.size()) {
      resourceValues = resources;
      return;
    }
    resourceValues = mergeRangeValues(valuesRequested, randomValues);
  }

  private List contiguousRange(long begin, long end) {
    ArrayList<Long> ret = new ArrayList<>();
    for (long i = begin; i <= end; i++) {
      ret.add(i);
    }
    return ret;
  }
}
