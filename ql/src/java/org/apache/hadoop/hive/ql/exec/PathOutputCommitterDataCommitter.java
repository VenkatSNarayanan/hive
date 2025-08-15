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

package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter;

import java.io.IOException;
import java.util.List;

/**
 * A {@link DataCommitter} that commits Hive data using a {@link PathOutputCommitter}.
 */
class PathOutputCommitterDataCommitter implements DataCommitter {

  private final JobContext jobContext;
  private final PathOutputCommitter pathOutputCommitter;

  PathOutputCommitterDataCommitter(JobContext jobContext,
                                          PathOutputCommitter pathOutputCommitter) {
    this.jobContext = jobContext;
    this.pathOutputCommitter = pathOutputCommitter;
  }

  @Override
  public void moveFile(Path sourcePath, Path targetPath, boolean isDfsDir, HiveConf conf,
                       SessionState.LogHelper console) throws HiveException {
    //commitJob();
    //These methods are no-ops because we want to call commitJob from FSPaths
    //need to look into whether this committer class is needed or if
    //a conditional block in the old methods are enough
  }

  @Override
  public void copyFiles(HiveConf conf, Path srcf, Path destf, FileSystem fs, boolean isSrcLocal,
                        boolean isAcidIUD, boolean isOverwrite, List<Path> newFiles,
                        boolean isBucketed, boolean isFullAcidTable,
                        boolean isManaged) throws HiveException {
    //commitJob();
  }

  @Override
  public void replaceFiles(Path tablePath, Path srcf, Path destf, Path oldPath, HiveConf conf,
                           boolean isSrcLocal, boolean purge, List<Path> newFiles,
                           PathFilter deletePathFilter, boolean isNeedRecycle, boolean isManaged,
                           Hive hive) throws HiveException {
    //commitJob();
  }

  private void commitJob() throws HiveException {
    try {
      this.pathOutputCommitter.commitJob(this.jobContext);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }
}
