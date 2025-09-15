/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.common.util;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collector;

public final class CustomCollectors {
  /**
   * Returns a {@code Collector} suitable for use with {@code Stream::collect} that collects input
   * elements into lists of up to size {@code partitionSize}. All but the final list are guaranteed
   * to have size equal to {@code partitionSize}; the final list may have a smaller size.
   *
   * @param partitionSize maximum size of sublists
   * @return {@code List<List<T>>} of input elements partitioned into sublists
   */
  public static <T> Collector<T, ?, List<List<T>>> toPartitionedList(int partitionSize) {
    class Accumulator {
      private final List<List<T>> result = new ArrayList<>();
      private List<T> current = new ArrayList<>(partitionSize);

      private void add(T t) {
        if (current.size() == partitionSize) {
          result.add(current);
          current = new ArrayList<>(partitionSize);
        }
        current.add(t);
      }

      private Accumulator merge(Accumulator other) {
        for (List<T> list : other.result) {
          list.forEach(this::add);
        }

        other.current.forEach(this::add);
        return this;
      }

      private List<List<T>> finish() {
        if (!current.isEmpty()) {
          result.add(current);
        }
        return result;
      }
    }

    return Collector.of(
        Accumulator::new, Accumulator::add, Accumulator::merge, Accumulator::finish);
  }
}
