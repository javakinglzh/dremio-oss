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
package com.dremio.dac.server;

import com.dremio.dac.model.job.JobDataFragment;
import com.dremio.dac.model.job.JobDataWrapper;
import com.dremio.exec.proto.UserBitShared;
import com.dremio.service.job.JobDetails;
import com.dremio.service.job.JobDetailsRequest;
import com.dremio.service.job.JobSummary;
import com.dremio.service.job.JobSummaryRequest;
import com.dremio.service.job.QueryProfileRequest;
import com.dremio.service.job.proto.JobAttempt;
import com.dremio.service.job.proto.JobId;
import com.dremio.service.job.proto.JobProtobuf;
import com.dremio.service.job.proto.JobSubmission;
import com.dremio.service.jobs.JobNotFoundException;
import com.dremio.service.jobs.JobRequest;
import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.JobsProtoUtil;
import com.dremio.service.jobs.JobsService;
import com.dremio.service.jobs.SqlQuery;
import com.google.errorprone.annotations.CanIgnoreReturnValue;
import com.google.errorprone.annotations.CheckReturnValue;
import com.google.errorprone.annotations.FormatMethod;
import com.google.errorprone.annotations.MustBeClosed;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.arrow.memory.BufferAllocator;

/**
 * Wrapper of {@link com.dremio.service.jobs.JobsService} with convenient helper methods for general
 * testing purposes.
 */
public class JobsServiceTestWrapper {
  private final JobsService jobService;
  private final String userName;

  public JobsServiceTestWrapper(JobsService jobsService, String userName) {
    this.jobService = jobsService;
    this.userName = userName;
  }

  private SqlQuery toSqlQuery(String query) {
    return toSqlQuery(query, null);
  }

  private SqlQuery toSqlQuery(String query, @Nullable String sessionId) {
    return new SqlQuery(query, List.of(), userName, null, sessionId);
  }

  private JobRequest toJobRequest(SqlQuery sqlQuery) {
    return JobRequest.newBuilder().setSqlQuery(sqlQuery).build();
  }

  @CanIgnoreReturnValue
  @FormatMethod
  public CompletedJob run(String query, Object... queryFormatArgs) {
    String fullQuery = String.format(query, queryFormatArgs);
    return run(fullQuery);
  }

  @CanIgnoreReturnValue
  public CompletedJob run(String query) {
    return run(toSqlQuery(query));
  }

  @CanIgnoreReturnValue
  public CompletedJob run(SqlQuery sqlQuery) {
    return run(toJobRequest(sqlQuery));
  }

  @CanIgnoreReturnValue
  public CompletedJob run(JobRequest jobRequest) {
    return run(jobRequest, JobStatusListener.NO_OP);
  }

  @CanIgnoreReturnValue
  public CompletedJob run(JobRequest jobRequest, JobStatusListener jobStatusListener) {
    JobSubmission jobSubmission =
        JobsServiceTestUtils.submitJobAndWaitUntilCompletion(
            jobService, jobRequest, jobStatusListener);
    return new CompletedJob(jobSubmission);
  }

  @CanIgnoreReturnValue
  public CompletedJob runInSession(String query, @Nullable String sessionId) {
    return run(toSqlQuery(query, sessionId));
  }

  @CanIgnoreReturnValue
  public JobId submit(JobRequest jobRequest) {
    return submit(jobRequest, JobStatusListener.NO_OP);
  }

  @CanIgnoreReturnValue
  public JobId submit(JobRequest jobRequest, JobStatusListener jobStatusListener) {
    return JobsServiceTestUtils.submitAndWaitUntilSubmitted(
        jobService, jobRequest, jobStatusListener);
  }

  @CheckReturnValue
  public boolean submitJobAndCancelOnTimeout(JobRequest jobRequest, long timeOutInMillis)
      throws Exception {
    return JobsServiceTestUtils.submitJobAndCancelOnTimeout(
        jobService, jobRequest, timeOutInMillis);
  }

  public void setSystemOption(String optionName, String optionValue) {
    run("ALTER SYSTEM SET \"%s\"=%s", optionName, optionValue);
  }

  public void resetSystemOption(String optionName) {
    run("ALTER SYSTEM RESET \"%s\"", optionName);
  }

  @CheckReturnValue
  public UserBitShared.QueryProfile getQueryProfile(JobSubmission jobSubmission, int attempt) {
    return getQueryProfile(jobSubmission.getJobId(), attempt);
  }

  @CheckReturnValue
  public UserBitShared.QueryProfile getQueryProfile(JobDetails jobDetails, int attempt) {
    return getQueryProfile(jobDetails.getJobId(), attempt);
  }

  @CheckReturnValue
  public UserBitShared.QueryProfile getQueryProfile(JobId jobId, int attempt) {
    return getQueryProfile(JobsProtoUtil.toBuf(jobId), attempt);
  }

  @CheckReturnValue
  public UserBitShared.QueryProfile getQueryProfile(JobProtobuf.JobId jobId, int attempt) {
    QueryProfileRequest queryProfileRequest =
        QueryProfileRequest.newBuilder()
            .setJobId(jobId)
            .setUserName(userName)
            .setAttempt(attempt)
            .build();
    try {
      return jobService.getProfile(queryProfileRequest);
    } catch (JobNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @CheckReturnValue
  public JobDetails getJobDetails(UserBitShared.QueryId queryId) {
    return getJobDetails(toJobId(queryId));
  }

  @CheckReturnValue
  public JobDetails getJobDetails(JobId jobId) {
    JobDetailsRequest jobDetailsRequest =
        JobDetailsRequest.newBuilder()
            .setJobId(JobsProtoUtil.toBuf(jobId))
            .setUserName(userName)
            .build();
    try {
      return jobService.getJobDetails(jobDetailsRequest);
    } catch (JobNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  @CheckReturnValue
  public JobSummary getJobSummary(JobId jobId) {
    JobSummaryRequest jobSummaryRequest =
        JobSummaryRequest.newBuilder()
            .setJobId(JobsProtoUtil.toBuf(jobId))
            .setUserName(userName)
            .build();
    try {
      return jobService.getJobSummary(jobSummaryRequest);
    } catch (JobNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static JobId toJobId(UserBitShared.QueryId queryId) {
    return new JobId(new UUID(queryId.getPart1(), queryId.getPart2()).toString());
  }

  /**
   * Intermediate representation of a completed job that can be used as an accessor to additional
   * information like job details, job summary, query profile, query data etc.
   */
  public class CompletedJob {
    private final JobSubmission jobSubmission;

    public CompletedJob(JobSubmission jobSubmission) {
      this.jobSubmission = jobSubmission;
    }

    @CheckReturnValue
    public JobSubmission getJobSubmission() {
      return jobSubmission;
    }

    @CheckReturnValue
    public JobId getJobId() {
      return jobSubmission.getJobId();
    }

    @CheckReturnValue
    public String getSessionId() {
      return jobSubmission.getSessionId().getId();
    }

    @CheckReturnValue
    public UserBitShared.QueryProfile getProfile() {
      return getProfile(0);
    }

    @CheckReturnValue
    public UserBitShared.QueryProfile getProfile(int attempt) {
      return JobsServiceTestWrapper.this.getQueryProfile(getJobId(), attempt);
    }

    @CheckReturnValue
    public JobSummary getJobSummary() {
      return JobsServiceTestWrapper.this.getJobSummary(getJobId());
    }

    @CheckReturnValue
    public JobDetails getJobDetails() {
      return JobsServiceTestWrapper.this.getJobDetails(getJobId());
    }

    @CheckReturnValue
    public JobAttempt getLastJobAttempt() {
      return JobsProtoUtil.getLastAttempt(getJobDetails());
    }

    @CheckReturnValue
    @MustBeClosed
    public JobDataFragment getData(int limit, BufferAllocator allocator) {
      return getData(0, limit, allocator);
    }

    @CheckReturnValue
    @MustBeClosed
    public JobDataFragment getData(int offset, int limit, BufferAllocator allocator) {
      return JobDataWrapper.getJobData(
          JobsServiceTestWrapper.this.jobService,
          allocator,
          jobSubmission.getJobId(),
          jobSubmission.getSessionId(),
          offset,
          limit);
    }
  }
}
