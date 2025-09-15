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

import com.dremio.common.exceptions.ErrorHelper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Decorator over AsyncByteReader with timeout. */
public class AsyncByteReaderWithTimeout extends ReusableAsyncByteReader {
  private static final Logger logger = LoggerFactory.getLogger(AsyncByteReaderWithTimeout.class);
  private static final ScheduledThreadPoolExecutor delayer;
  private final AtomicInteger numOutstandingReads = new AtomicInteger(0);
  private final AsyncByteReader inner;
  private final long timeoutInMillis;

  // Metrics tracking
  private static final AtomicLong totalTimeoutTasksScheduled = new AtomicLong(0);
  private static final AtomicLong totalTimeoutTasksCompleted = new AtomicLong(0);
  private static final AtomicLong totalTimeoutTasksCancelled = new AtomicLong(0);

  static {
    delayer =
        new ScheduledThreadPoolExecutor(
            1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("timeoutAfter-%d").build());
    // remove cancelled tasks from the queue to reduce heap usage.
    delayer.setRemoveOnCancelPolicy(true);
  }

  public AsyncByteReaderWithTimeout(
      AsyncByteReader inner, long timeoutInMillis, int delayerThreadCount) {
    this.inner = inner;
    this.timeoutInMillis = timeoutInMillis;
    delayer.setCorePoolSize(delayerThreadCount);
  }

  /**
   * Wrapper over TimeoutException, just to distinguish from the case that S3/azure clients also
   * generate a TimeoutException. In that case, the buffer can be safely released.
   */
  private static class AsyncTimeoutException extends TimeoutException {
    AsyncTimeoutException() {
      super();
    }
  }

  @Override
  public CompletableFuture<Void> readFully(long offset, ByteBuf dst, int dstOffset, int len) {
    numOutstandingReads.getAndIncrement();
    CompletableFuture<Void> future =
        within(inner.readFully(offset, dst, dstOffset, len), timeoutInMillis);
    future =
        future.whenComplete(
            (result, throwable) -> {
              if (ErrorHelper.findWrappedCause(throwable, AsyncTimeoutException.class) != null) {
                // grab an extra ref on behalf of the read that may complete at any point
                // in the future and access the buf. This buf is essentially leaked.
                dst.retain();
              }
              numOutstandingReads.getAndDecrement();
            });
    if (logger.isDebugEnabled()) {
      logger.debug(getThreadPoolStats());
    }
    return future;
  }

  /** if the future cannot complete within 'millis', fail with TimeoutException. */
  private static <T> CompletableFuture<T> within(CompletableFuture<T> future, long millis) {
    // Track that we're scheduling a timeout task
    totalTimeoutTasksScheduled.incrementAndGet();

    // schedule a task to generate a timeout after 'millis'
    final CompletableFuture<T> timeout = new CompletableFuture<>();
    ScheduledFuture timeoutTask =
        delayer.schedule(
            () -> {
              // Track that the timeout was reached
              totalTimeoutTasksCompleted.incrementAndGet();
              timeout.completeExceptionally(new AsyncTimeoutException());
            },
            millis,
            TimeUnit.MILLISECONDS);

    // accept either the origin future or the timeout, which ever happens first. cancel the timeout
    // task in either case.
    return future
        .applyToEither(timeout, Function.identity())
        .whenComplete(
            (x, y) -> {
              if (timeoutTask.cancel(true) && y == null) {
                // Track that the timout task was cancelled because the wrapped future was completed
                totalTimeoutTasksCancelled.incrementAndGet();
              }
              if (logger.isDebugEnabled()) {
                logger.debug(getThreadPoolStats());
              }
            });
  }

  @Override
  protected void onClose() throws Exception {
    inner.close();
  }

  @Override
  public List<ReaderStat> getStats() {
    return inner.getStats();
  }

  /**
   * Get current thread pool statistics for monitoring.
   *
   * @return formatted string with thread pool metrics
   */
  @VisibleForTesting
  static String getThreadPoolStats() {
    if (delayer == null) {
      return "Thread pool not initialized";
    }

    return String.format(
        "AsyncByteReaderWithTimeout ThreadPool Stats: "
            + "queueSize=%d, activeThreads=%d, corePoolSize=%d, "
            + "completedTasks=%d, totalTasks=%d, "
            + "customScheduled=%d, customCompleted=%d, customCancelled=%d",
        delayer.getQueue().size(),
        delayer.getActiveCount(),
        delayer.getCorePoolSize(),
        delayer.getCompletedTaskCount(),
        delayer.getTaskCount(),
        totalTimeoutTasksScheduled.get(),
        totalTimeoutTasksCompleted.get(),
        totalTimeoutTasksCancelled.get());
  }
}
