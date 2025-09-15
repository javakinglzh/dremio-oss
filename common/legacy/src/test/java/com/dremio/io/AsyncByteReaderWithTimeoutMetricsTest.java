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
package com.dremio.io;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import org.junit.Test;

public class AsyncByteReaderWithTimeoutMetricsTest {

  private static final ThreadPoolExecutor executor =
      new ThreadPoolExecutor(
          1,
          5,
          0,
          java.util.concurrent.TimeUnit.SECONDS,
          new java.util.concurrent.LinkedBlockingQueue<>());

  @Test
  public void testMetricsTracking() throws Exception {
    // Create AsyncByteReaderWithTimeout
    try (AsyncByteReaderWithTimeout readerWithTimeout =
        new AsyncByteReaderWithTimeout(getMockReader(1000), 60000, 1)) {
      // Get initial stats
      String initialStats = AsyncByteReaderWithTimeout.getThreadPoolStats();
      System.out.println("Initial stats: " + initialStats);
      assertThat(initialStats).contains("corePoolSize=1");

      // Perform some reads to generate metrics
      ByteBuf buffer = Unpooled.buffer(100);
      try {
        for (int i = 0; i < 5; i++) {
          readerWithTimeout.readFully(0, buffer, 0, 10).get();
        }

        // Get final stats
        String finalStats = AsyncByteReaderWithTimeout.getThreadPoolStats();
        System.out.println("Final stats: " + finalStats);

        assertThat(finalStats).contains("corePoolSize=1");
        // timeout tasks scheduled
        assertThat(finalStats).contains("customScheduled=5");
        // timeout tasks cancelled
        assertThat(finalStats).contains("customCancelled=5");

      } finally {
        buffer.release();
      }
    }

    // Create AsyncByteReaderWithTimeout with 50ms timeout
    try (AsyncByteReaderWithTimeout readerWithTimeout =
        new AsyncByteReaderWithTimeout(getMockReader(100), 50, 1)) {
      // Get initial stats
      String initialStats = AsyncByteReaderWithTimeout.getThreadPoolStats();
      System.out.println("Initial stats: " + initialStats);
      assertThat(initialStats).contains("corePoolSize=1");

      // Perform one read to generate metrics with timeout
      ByteBuf buffer = Unpooled.buffer(100);
      try {
        assertThatThrownBy(() -> readerWithTimeout.readFully(0, buffer, 0, 10).get())
            .isInstanceOf(ExecutionException.class);

        // Get final stats
        String finalStats = AsyncByteReaderWithTimeout.getThreadPoolStats();
        System.out.println("Final stats: " + finalStats);

        assertThat(finalStats).contains("corePoolSize=1");
        // timeout tasks completed
        assertThat(finalStats).contains("customCompleted=1");
        // total tasks completed
        assertThat(finalStats).contains("completedTasks=1");
        // total tasks scheduled
        assertThat(finalStats).contains("totalTasks=1");
        // queue size
        assertThat(finalStats).contains("queueSize=0");
        // active threads
        assertThat(finalStats).contains("activeThreads=0");
        // timeout tasks scheduled
        assertThat(finalStats).contains("customScheduled=6");
        // timeout tasks cancelled
        assertThat(finalStats).contains("customCancelled=5");
      } finally {
        buffer.release();
      }
    }
  }

  private AsyncByteReader getMockReader(long sleepTime) {
    return new ReusableAsyncByteReader() {
      @Override
      public CompletableFuture<Void> readFully(long offset, ByteBuf dst, int dstOffset, int len) {
        return CompletableFuture.runAsync(
            () -> {
              try {
                Thread.sleep(sleepTime);
              } catch (InterruptedException e) {
                e.printStackTrace();
              }
            },
            executor);
      }

      @Override
      protected void onClose() throws Exception {
        // No-op
      }
    };
  }
}
