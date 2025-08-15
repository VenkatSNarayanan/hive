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

package org.apache.hadoop.hive.ql.optimizer.physical;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.CommitConstants;
import org.apache.hadoop.fs.s3a.commit.InternalCommitterConstants;

import org.apache.hadoop.hive.common.BlobStorageUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.MoveTask;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.PathOutputCommitterSetupTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.exec.mr.MapRedTask;
import org.apache.hadoop.hive.ql.exec.spark.SparkTask;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.TaskGraphWalker;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.plan.PathOutputCommitterWork;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.lib.output.PathOutputCommitterFactory;
import org.apache.hadoop.mapreduce.task.JobContextImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.UUID;
import java.util.stream.Collectors;


/**
 * This {@link PhysicalPlanResolver} should only be triggered if
 * {@link HiveConf.ConfVars#HIVE_BLOBSTORE_USE_OUTPUTCOMMITTER} is set to true. This class is
 * used to integrate Hive with a specified
 * {@link org.apache.hadoop.mapreduce.lib.output.PathOutputCommitter}. When triggered
 * the {@link #resolve(PhysicalContext)} method will modify the operator and task DAGs so that
 * they run all necessary stages of the specified PathOutputCommitter. Since Output Committers
 * specify how data should be committed, this class only modifies query plans that write data.
 * Integration with Output Committers is done via modifying the {@link FileSinkOperator} and
 * {@link MoveTask} as well as introducing a {@link PathOutputCommitterSetupTask} to the task DAG.
 *
 * <p>
 *   This class currently has the following restrictions: (1) it is not triggered for dynamic
 *   partitioning queries, and (2) it is not triggered when the merge-small-files job is enabled.
 * </p>
 */
public class PathOutputCommitterResolver implements PhysicalPlanResolver {

  private static final Logger LOG = LoggerFactory.getLogger(PathOutputCommitterResolver.class);

  // Useful for testing
  private static final String HIVE_BLOBSTORE_COMMIT_DISABLE_EXPLAIN = "hive.blobstore.commit." +
          "disable.explain";

  private final Map<Task<?>, Collection<FileSinkOperator>> taskToFsOps = new HashMap<>();
  private final List<Task<MoveWork>> mvTasks = new ArrayList<>();
  private HiveConf hconf;

  @Override
  public PhysicalContext resolve(PhysicalContext pctx) throws SemanticException {
    this.hconf = pctx.getConf();

    // Collect all MoveTasks and FSOPs
    TaskGraphWalker graphWalker = new TaskGraphWalker(new PathOutputCommitterDispatcher());
    List<Node> rootTasks = new ArrayList<>(pctx.getRootTasks());
    graphWalker.startWalking(rootTasks, null);

    // Find MoveTasks with no child MoveTask
    List<Task<MoveWork>> sinkMoveTasks = mvTasks.stream()
            .filter(mvTask -> !containsChildTask(mvTask.getChildTasks(), MoveTask.class))
            .collect(Collectors.toList());

    // Iterate through each FSOP
    for (Map.Entry<Task<?>, Collection<FileSinkOperator>> entry : taskToFsOps.entrySet()) {
      for (FileSinkOperator fsOp : entry.getValue()) {
        try {
          processFsOp(entry.getKey(), fsOp, sinkMoveTasks);
        } catch (HiveException | MetaException e) {
          throw new SemanticException(e);
        }
      }
    }

    return pctx;
  }

  private boolean containsChildTask(List<Task<? extends Serializable>> mvTasks, Class<MoveTask>
          taskClass) {
    if (mvTasks == null) {
      return false;
    }
    boolean containsChildTask = false;
    for (Task<? extends Serializable> mvTask : mvTasks) {
      if (taskClass.isInstance(mvTask)) {
        return true;
      }
      containsChildTask = containsChildTask(mvTask.getChildTasks(), taskClass);
    }
    return containsChildTask;
  }

  private class PathOutputCommitterDispatcher implements Dispatcher {

    @Override
    public Object dispatch(Node nd, Stack<Node> stack,
                           Object... nodeOutputs) throws SemanticException {

      Task<?> task = (Task<?>) nd;
      Collection<FileSinkOperator> fsOps = getAllFsOps(task);
      if (!fsOps.isEmpty()) {
        taskToFsOps.put((Task<?>) nd, fsOps);
      }
      if (nd instanceof MoveTask) {
        mvTasks.add((MoveTask) nd);
      }
      return null;
    }
  }

  private Collection<FileSinkOperator> getAllFsOps(Task<?> task) {
    Collection<Operator<?>> fsOps = new ArrayList<>();
    if (task instanceof MapRedTask) {
      fsOps.addAll(((MapRedTask) task).getWork().getAllOperators());
    } else if (task instanceof SparkTask) {
      for (BaseWork work : ((SparkTask) task).getWork().getAllWork()) {
        fsOps.addAll(work.getAllOperators());
      }
    } else if (task instanceof TezTask) {
      for (BaseWork work : ((TezTask) task).getWork().getAllWork()) {
        fsOps.addAll(work.getAllOperators());
      }
    }
    return fsOps.stream()
            .filter(FileSinkOperator.class::isInstance)
            .map(FileSinkOperator.class::cast)
            .collect(Collectors.toList());
  }

  private void processFsOp(Task<?> task, FileSinkOperator fsOp,
                           List<Task<MoveWork>> sinkMoveTasks) throws HiveException, MetaException {
    FileSinkDesc fileSinkDesc = fsOp.getConf();

    // Get the MoveTask that will process the output of the fsOp
    Task<MoveWork> mvTask = GenMapRedUtils.findMoveTaskForFsopOutput(sinkMoveTasks,
            fileSinkDesc.getFinalDirName(), fileSinkDesc.isMmTable());

    if (mvTask != null) {

      MoveWork mvWork = mvTask.getWork();

      if (mvWork.getLoadMultiFilesWork() == null) {

        // The final output path we will commit data to
        Path outputPath = null;

        // Instead of picking between load table work and load file work, throw an exception if
        // they are both set (this should never happen)
        if (mvWork.getLoadTableWork() != null && mvWork.getLoadFileWork() != null) {
          throw new IllegalArgumentException("Load Table Work and Load File Work cannot both be " +
                  "set");
        }

        // If there is a load file work, get its output path
        if (mvWork.getLoadFileWork() != null) {
          outputPath = getLoadFileOutputPath(mvWork);
        }

        // If there is a load table work, get is output path
        if (mvTask.getWork().getLoadTableWork() != null) {
          outputPath = getLoadTableOutputPath(mvWork);
        }
        if (outputPath != null) {
          if (BlobStorageUtils.isBlobStoragePath(hconf, outputPath) && hconf.get(
                  String.format(PathOutputCommitterFactory.COMMITTER_FACTORY_SCHEME_PATTERN,
                          outputPath.toUri().getScheme())) != null) {

            // All s3a specific logic should be place in the method below, for all filesystems or
            // output committer implementations, a similar pattern should be followed
            if ("s3a".equals(outputPath.toUri().getScheme())) {
              setupS3aOutputCommitter(mvWork);
            }

            PathOutputCommitterWork setupWork = createPathOutputCommitterWork(outputPath);
            mvWork.setPathOutputCommitterWork(setupWork);

            fileSinkDesc.setHasOutputCommitter(true);
            fileSinkDesc.setTargetDirName(outputPath.toString());

            LOG.info("Using Output Committer " + setupWork.getPathOutputCommitterClass() +
                    " for MoveTask: " + mvTask + ", FileSinkOperator: " + fsOp + " and output " +
                    "path: " + outputPath);

            if (hconf.getBoolean(HIVE_BLOBSTORE_COMMIT_DISABLE_EXPLAIN, false)) {
              PathOutputCommitterSetupTask setupTask = new PathOutputCommitterSetupTask();
              setupTask.setWork(setupWork);
              setupTask.executeTask(null);
            } else {
              task.addDependentTask(TaskFactory.get(setupWork));
            }
          }
        }
      }
    }
  }

  private PathOutputCommitterWork createPathOutputCommitterWork(Path outputPath) {
    JobID jobID = new JobID();
    JobContext jobContext = new JobContextImpl(hconf, jobID);

    return new PathOutputCommitterWork(outputPath.toString(),
            jobContext);
  }

  /**
   * Setups any necessary configuration specific to a s3a. All s3a specific logic should be
   * encapsulated within this method.
   */
  private void setupS3aOutputCommitter(MoveWork mvWork) { //modify this for whatever magic committer needs
    // Since the Hive-PathOutputCommitter integration is done manually (e.g. not through a
    // framework such as MR or Spark) there is no auto-assigned staging ID, so we add one ourselves
    hconf.set("fs.s3a.committer.uuid", UUID.randomUUID().toString());
  }

  // Somewhat copied from TaskCompiler, which uses similar logic to get the default location for
  // the target table in a CTAS query
  private Path getDefaultPartitionPath(Path tablePath, Map<String, String> partitionSpec)
          throws MetaException {
    Warehouse wh = new Warehouse(hconf);
    return wh.getPartitionPath(tablePath, partitionSpec);
  }

  private Path getLoadFileOutputPath(MoveWork mvWork) {
    return mvWork.getLoadFileWork().getTargetDir();
  }

  private Path getLoadTableOutputPath(MoveWork mvWork) throws HiveException, MetaException {
    if (mvWork.getLoadTableWork().getPartitionSpec() != null &&
            !mvWork.getLoadTableWork().getPartitionSpec().isEmpty()) {
      if (mvWork.getLoadTableWork().getDPCtx() == null) {
        return getLoadPartitionOutputPath(mvWork);
      }
      else {
        return mvWork.getLoadTableWork().getDPCtx().getRootPath();
      }
    } else {
      // should probably replace this with Hive.getTable().getDataLocation()
      return new Path(mvWork.getLoadTableWork().getTable().getProperties()
              .getProperty("location")); // INSERT INTO ... VALUES (...)
    }
  }

  private Path getLoadPartitionOutputPath(MoveWork mvWork) throws HiveException, MetaException {
    Hive db = Hive.get();
    Partition partition = db.getPartition(db.getTable(mvWork.getLoadTableWork()
                    .getTable().getTableName()),
            mvWork.getLoadTableWork().getPartitionSpec(), false);
    if (partition != null) {
      return partition.getDataLocation();
    } else {
      return getDefaultPartitionPath(db.getTable(mvWork.getLoadTableWork()
              .getTable().getTableName()).getDataLocation(), mvWork
              .getLoadTableWork().getPartitionSpec());
    }
  }
}