/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;

import java.io.IOException;
import java.io.Serializable;


@Explain(displayName = "Path Output Committer Setup Work", explainLevels = {Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED})
public class PathOutputCommitterWork implements Serializable {

  private static final long serialVersionUID = -6333040835478371176L;

  private String outputPath;
  private transient JobContext jobContext;

  public PathOutputCommitterWork(String outputPath, JobContext jobContext) {
    this.outputPath = outputPath;
    this.jobContext = jobContext;
  }

  @Explain(displayName = "Path Output Committer Factory")
  public Class<? extends PathOutputCommitterFactory> getPathOutputCommitterClass() {
    return PathOutputCommitterFactory.getCommitterFactory(new Path(this.outputPath), this.jobContext
            .getConfiguration()).getClass();
  }

  @Explain(displayName = "Path Output Path")
  public String getOutputPath() {
    return this.outputPath;
  }

  public JobContext getJobContext() {
    return this.jobContext;
  }

  public PathOutputCommitter createPathOutputCommitter() throws IOException {
    return PathOutputCommitterFactory.createCommitter(new Path(this.outputPath),
            this.jobContext);
  }
}
