package org.apache.hadoop.hive.ql.exec;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.commons.io.FilenameUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.common.ObjectPair;
import org.apache.hadoop.hive.common.classification.InterfaceAudience;
import org.apache.hadoop.hive.common.classification.InterfaceStability;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.io.HdfsUtils;
import org.apache.hadoop.hive.metastore.api.CmRecycleRequest;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MoveWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.HadoopShims;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A {@link DataCommitter} that commits Hive data using a {@link FileSystem}.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class HiveDataCommitter implements DataCommitter {

  private static final Logger LOG = LoggerFactory.getLogger(DataCommitter.class.getName());

  private MoveWork work;

  HiveDataCommitter(MoveWork moveWork) {
    this.work = moveWork;
  }

  @VisibleForTesting
  public HiveDataCommitter() {
    // Do nothing
  }

  @Override
  public void moveFile(Path sourcePath, Path targetPath, boolean isDfsDir, HiveConf conf,
                       SessionState.LogHelper console) throws HiveException {
    try {
      PerfLogger perfLogger = SessionState.getPerfLogger();
      perfLogger.PerfLogBegin("MoveTask", "FileMoves");

      String mesg = "Moving data to " + (isDfsDir ? "" : "local ") + "directory "
          + targetPath.toString();
      String mesg_detail = " from " + sourcePath.toString();
      console.printInfo(mesg, mesg_detail);

      FileSystem fs = sourcePath.getFileSystem(conf);
      if (isDfsDir) {
        moveFileInDfs (sourcePath, targetPath, conf);
      } else {
        // This is a local file
        FileSystem dstFs = FileSystem.getLocal(conf);
        moveFileFromDfsToLocal(sourcePath, targetPath, fs, dstFs, conf);
      }

      perfLogger.PerfLogEnd("MoveTask", "FileMoves");
    } catch (Exception e) {
      throw new HiveException("Unable to move source " + sourcePath + " to destination " + targetPath, e);
    }
  }

   private void moveFileInDfs (Path sourcePath, Path targetPath, HiveConf conf)
      throws HiveException, IOException {

    final FileSystem srcFs, tgtFs;
    try {
      tgtFs = targetPath.getFileSystem(conf);
    } catch (IOException e) {
      LOG.error("Failed to get dest fs", e);
      throw new HiveException(e.getMessage(), e);
    }
    try {
      srcFs = sourcePath.getFileSystem(conf);
    } catch (IOException e) {
      LOG.error("Failed to get src fs", e);
      throw new HiveException(e.getMessage(), e);
    }

    // if source exists, rename. Otherwise, create a empty directory
    if (srcFs.exists(sourcePath)) {
      Path deletePath = null;
      // If it multiple level of folder are there fs.rename is failing so first
      // create the targetpath.getParent() if it not exist
      if (HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_INSERT_INTO_MULTILEVEL_DIRS)) {
        deletePath = createTargetPath(targetPath, tgtFs);
      }
      //For acid table incremental replication, just copy the content of staging directory to destination.
      //No need to clean it.
      if (work.isNeedCleanTarget()) {
        Hive.clearDestForSubDirSrc(conf, targetPath, sourcePath, false);
      }
      // Set isManaged to false as this is not load data operation for which it is needed.
      if (!moveFile(conf, sourcePath, targetPath, true, false, false)) {
        try {
          if (deletePath != null) {
            tgtFs.delete(deletePath, true);
          }
        } catch (IOException e) {
          LOG.info("Unable to delete the path created for facilitating rename: {}",
            deletePath);
        }
        throw new HiveException("Unable to rename: " + sourcePath
            + " to: " + targetPath);
      }
    } else if (!tgtFs.mkdirs(targetPath)) {
      throw new HiveException("Unable to make directory: " + targetPath);
    }
  }

  private void moveFileFromDfsToLocal(Path sourcePath, Path targetPath, FileSystem fs,
                                      FileSystem dstFs,
                                      Configuration conf) throws HiveException, IOException {
      // RawLocalFileSystem seems not able to get the right permissions for a local file, it
      // always returns hdfs default permission (00666). So we can not overwrite a directory
      // by deleting and recreating the directory and restoring its permissions. We should
      // delete all its files and subdirectories instead.
    if (dstFs.exists(targetPath)) {
      if (dstFs.isDirectory(targetPath)) {
        FileStatus[] destFiles = dstFs.listStatus(targetPath);
        for (FileStatus destFile : destFiles) {
          if (!dstFs.delete(destFile.getPath(), true)) {
            throw new IOException("Unable to clean the destination directory: " + targetPath);
          }
        }
      } else {
        throw new HiveException("Target " + targetPath + " is not a local directory.");
      }
    } else {
      if (!FileUtils.mkdir(dstFs, targetPath, conf)) {
        throw new HiveException("Failed to create local target directory " + targetPath);
      }
    }

    if (fs.exists(sourcePath)) {
      FileStatus[] srcs = fs.listStatus(sourcePath, FileUtils.HIDDEN_FILES_PATH_FILTER);
      for (FileStatus status : srcs) {
        fs.copyToLocalFile(status.getPath(), targetPath);
      }
    }
  }

  private Path createTargetPath(Path targetPath, FileSystem fs) throws IOException {
    Path deletePath = null;
    Path mkDirPath = targetPath.getParent();
    if (mkDirPath != null && !fs.exists(mkDirPath)) {
      Path actualPath = mkDirPath;
      // targetPath path is /x/y/z/1/2/3 here /x/y/z is present in the file system
      // create the structure till /x/y/z/1/2 to work rename for multilevel directory
      // and if rename fails delete the path /x/y/z/1
      // If targetPath have multilevel directories like /x/y/z/1/2/3 , /x/y/z/1/2/4
      // the renaming of the directories are not atomic the execution will happen one
      // by one
      while (actualPath != null && !fs.exists(actualPath)) {
        deletePath = actualPath;
        actualPath = actualPath.getParent();
      }
      fs.mkdirs(mkDirPath);
    }
    return deletePath;
  }

  //it is assumed that parent directory of the destf should already exist when this
  //method is called. when the replace value is true, this method works a little different
  //from mv command if the destf is a directory, it replaces the destf instead of moving under
  //the destf. in this case, the replaced destf still preserves the original destf's permission
  private boolean moveFile(HiveConf conf, Path srcf, Path destf, boolean replace,
                          boolean isSrcLocal, boolean isManaged) throws HiveException {
    final FileSystem srcFs, destFs;
    try {
      destFs = destf.getFileSystem(conf);
    } catch (IOException e) {
      LOG.error("Failed to get dest fs", e);
      throw new HiveException(e.getMessage(), e);
    }
    try {
      srcFs = srcf.getFileSystem(conf);
    } catch (IOException e) {
      LOG.error("Failed to get src fs", e);
      throw new HiveException(e.getMessage(), e);
    }

    HdfsUtils.HadoopFileStatus destStatus = null;
    String configuredOwner = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_LOAD_DATA_OWNER);

    // If source path is a subdirectory of the destination path (or the other way around):
    //   ex: INSERT OVERWRITE DIRECTORY 'target/warehouse/dest4.out' SELECT src.value WHERE src.key >= 300;
    //   where the staging directory is a subdirectory of the destination directory
    // (1) Do not delete the dest dir before doing the move operation.
    // (2) It is assumed that subdir and dir are in same encryption zone.
    // (3) Move individual files from scr dir to dest dir.
    boolean srcIsSubDirOfDest = Hive.isSubDir(srcf, destf, srcFs, destFs, isSrcLocal),
        destIsSubDirOfSrc = Hive.isSubDir(destf, srcf, destFs, srcFs, false);
    final String msg = "Unable to move source " + srcf + " to destination " + destf;
    try {
      if (replace) {
        try{
          destStatus = new HdfsUtils.HadoopFileStatus(conf, destFs, destf);
          //if destf is an existing directory:
          //if replace is true, delete followed by rename(mv) is equivalent to replace
          //if replace is false, rename (mv) actually move the src under dest dir
          //if destf is an existing file, rename is actually a replace, and do not need
          // to delete the file first
          if (replace && !srcIsSubDirOfDest) {
            destFs.delete(destf, true);
            LOG.debug("The path " + destf.toString() + " is deleted");
          }
        } catch (FileNotFoundException ignore) {
        }
      }
      final HdfsUtils.HadoopFileStatus desiredStatus = destStatus;
      final SessionState parentSession = SessionState.get();
      if (isSrcLocal) {
        // For local src file, copy to hdfs
        destFs.copyFromLocalFile(srcf, destf);
        return true;
      } else {
        if (needToCopy(srcf, destf, srcFs, destFs, configuredOwner, isManaged)) {
          //copy if across file system or encryption zones.
          LOG.debug("Copying source " + srcf + " to " + destf + " because HDFS encryption zones are different.");
          return FileUtils.copy(srcf.getFileSystem(conf), srcf, destf.getFileSystem(conf), destf,
              true,    // delete source
              replace, // overwrite destination
              conf);
        } else {
          if (srcIsSubDirOfDest || destIsSubDirOfSrc) {
            FileStatus[] srcs = destFs.listStatus(srcf, FileUtils.HIDDEN_FILES_PATH_FILTER);

            List<Future<Void>> futures = new LinkedList<>();
            final ExecutorService pool = conf.getInt(HiveConf.ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 25) > 0 ?
                Executors.newFixedThreadPool(conf.getInt(HiveConf.ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 25),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Move-Thread-%d").build()) : null;
            if (destIsSubDirOfSrc && !destFs.exists(destf)) {
              if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
                Utilities.FILE_OP_LOGGER.trace("Creating " + destf);
              }
              destFs.mkdirs(destf);
            }
            /* Move files one by one because source is a subdirectory of destination */
            for (final FileStatus srcStatus : srcs) {

              final Path destFile = new Path(destf, srcStatus.getPath().getName());

              final String poolMsg =
                  "Unable to move source " + srcStatus.getPath() + " to destination " + destFile;

              if (null == pool) {
                boolean success = false;
                if (destFs instanceof DistributedFileSystem) {
                  ((DistributedFileSystem)destFs).rename(srcStatus.getPath(), destFile, Options.Rename.OVERWRITE);
                  success = true;
                } else {
                  destFs.delete(destFile, false);
                  success = destFs.rename(srcStatus.getPath(), destFile);
                }
                if(!success) {
                  throw new IOException("rename for src path: " + srcStatus.getPath() + " to dest:"
                      + destf + " returned false");
                }
              } else {
                futures.add(pool.submit(new Callable<Void>() {
                  @Override
                  public Void call() throws HiveException {
                    SessionState.setCurrentSessionState(parentSession);
                    try {
                      boolean success = false;
                      if (destFs instanceof DistributedFileSystem) {
                        ((DistributedFileSystem)destFs).rename(srcStatus.getPath(), destFile, Options.Rename.OVERWRITE);
                        success = true;
                      } else {
                        destFs.delete(destFile, false);
                        success = destFs.rename(srcStatus.getPath(), destFile);
                      }
                      if (!success) {
                        throw new IOException(
                            "rename for src path: " + srcStatus.getPath() + " to dest path:"
                                + destFile + " returned false");
                      }
                    } catch (Exception e) {
                      throw Hive.getHiveException(e, poolMsg);
                    }
                    return null;
                  }
                }));
              }
            }
            if (null != pool) {
              pool.shutdown();
              for (Future<Void> future : futures) {
                try {
                  future.get();
                } catch (Exception e) {
                  throw handlePoolException(pool, e);
                }
              }
            }
            return true;
          } else {
            if (destFs.rename(srcf, destf)) {
              return true;
            }
            return false;
          }
        }
      }
    } catch (Exception e) {
      throw Hive.getHiveException(e, msg);
    }
  }

  /**
   * Copy files.  This handles building the mapping for buckets and such between the source and
   * destination
   * @param conf Configuration object
   * @param srcf source directory, if bucketed should contain bucket files
   * @param destf directory to move files into
   * @param fs Filesystem
   * @param isSrcLocal true if source is on local file system
   * @param isAcidIUD true if this is an ACID based Insert/Update/Delete
   * @param isOverwrite if true, then overwrite if destination file exist, else add a duplicate copy
   * @param newFiles if this is non-null, a list of files that were created as a result of this
   *                 move will be returned.
   * @param isManaged if table is managed.
   * @throws HiveException
   */
  @Override
  public void copyFiles(HiveConf conf, Path srcf, Path destf, FileSystem fs,
                                  boolean isSrcLocal, boolean isAcidIUD,
                                  boolean isOverwrite, List<Path> newFiles, boolean isBucketed,
                                  boolean isFullAcidTable, boolean isManaged) throws HiveException {
    try {
      // create the destination if it does not exist
      if (!fs.exists(destf)) {
        FileUtils.mkdir(fs, destf, conf);
      }
    } catch (IOException e) {
      throw new HiveException(
          "copyFiles: error while checking/creating destination directory!!!",
          e);
    }

    FileStatus[] srcs;
    FileSystem srcFs;
    try {
      srcFs = srcf.getFileSystem(conf);
      srcs = srcFs.globStatus(srcf);
    } catch (IOException e) {
      LOG.error(StringUtils.stringifyException(e));
      throw new HiveException("addFiles: filesystem error in check phase. " + e.getMessage(), e);
    }
    if (srcs == null) {
      LOG.info("No sources specified to move: " + srcf);
      return;
      // srcs = new FileStatus[0]; Why is this needed?
    }

    // If we're moving files around for an ACID write then the rules and paths are all different.
    // You can blame this on Owen.
    if (isAcidIUD) {
      Hive.moveAcidFiles(srcFs, srcs, destf, newFiles);
    } else {
      // For ACID non-bucketed case, the filenames have to be in the format consistent with INSERT/UPDATE/DELETE Ops,
      // i.e, like 000000_0, 000001_0_copy_1, 000002_0.gz etc.
      // The extension is only maintained for files which are compressed.
      copyFiles(conf, fs, srcs, srcFs, destf, isSrcLocal, isOverwrite,
              newFiles, isFullAcidTable && !isBucketed, isManaged);
    }
  }

  private void copyFiles(HiveConf conf, FileSystem destFs, FileStatus[] srcs,
                        FileSystem srcFs, Path destf, boolean isSrcLocal,
                        boolean isOverwrite, List<Path> newFiles,
                        boolean acidRename, boolean isManaged) throws HiveException {
    final HdfsUtils.HadoopFileStatus fullDestStatus;
    try {
      fullDestStatus = new HdfsUtils.HadoopFileStatus(conf, destFs, destf);
    } catch (IOException e1) {
      throw new HiveException(e1);
    }

    if (!fullDestStatus.getFileStatus().isDirectory()) {
      throw new HiveException(destf + " is not a directory.");
    }
    final List<Future<ObjectPair<Path, Path>>> futures = new LinkedList<>();
    final ExecutorService pool = conf.getInt(HiveConf.ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 25) > 0 ?
        Executors.newFixedThreadPool(conf.getInt(HiveConf.ConfVars.HIVE_MOVE_FILES_THREAD_COUNT.varname, 25),
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Move-Thread-%d").build()) : null;
    // For ACID non-bucketed case, the filenames have to be in the format consistent with INSERT/UPDATE/DELETE Ops,
    // i.e, like 000000_0, 000001_0_copy_1, 000002_0.gz etc.
    // The extension is only maintained for files which are compressed.
    int taskId = 0;
    // Sort the files
    Arrays.sort(srcs);
    String configuredOwner = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_LOAD_DATA_OWNER);
    for (FileStatus src : srcs) {
      FileStatus[] files;
      if (src.isDirectory()) {
        try {
          files = srcFs.listStatus(src.getPath(), FileUtils.HIDDEN_FILES_PATH_FILTER);
        } catch (IOException e) {
          if (null != pool) {
            pool.shutdownNow();
          }
          throw new HiveException(e);
        }
      } else {
        files = new FileStatus[] {src};
      }

      final SessionState parentSession = SessionState.get();
      // Sort the files
      Arrays.sort(files);
      for (final FileStatus srcFile : files) {
        final Path srcP = srcFile.getPath();
        final boolean needToCopy = needToCopy(srcP, destf, srcFs, destFs, configuredOwner, isManaged);

        final boolean isRenameAllowed = !needToCopy && !isSrcLocal;

        final String msg = "Unable to move source " + srcP + " to destination " + destf;

        // If we do a rename for a non-local file, we will be transfering the original
        // file permissions from source to the destination. Else, in case of mvFile() where we
        // copy from source to destination, we will inherit the destination's parent group ownership.
        if (null == pool) {
          try {
            Path destPath = mvFile(conf, srcFs, srcP, destFs, destf, isSrcLocal, isOverwrite, isRenameAllowed,
                    acidRename ? taskId++ : -1);

            if (null != newFiles) {
              newFiles.add(destPath);
            }
          } catch (Exception e) {
            throw Hive.getHiveException(e, msg, "Failed to move: {}");
          }
        } else {
          // future only takes final or seemingly final values. Make a final copy of taskId
          final int finalTaskId = acidRename ? taskId++ : -1;
          futures.add(pool.submit(new Callable<ObjectPair<Path, Path>>() {
            @Override
            public ObjectPair<Path, Path> call() throws HiveException {
              SessionState.setCurrentSessionState(parentSession);

              try {
                Path destPath =
                    mvFile(conf, srcFs, srcP, destFs, destf, isSrcLocal, isOverwrite, isRenameAllowed, finalTaskId);

                if (null != newFiles) {
                  newFiles.add(destPath);
                }
                return ObjectPair.create(srcP, destPath);
              } catch (Exception e) {
                throw Hive.getHiveException(e, msg);
              }
            }
          }));
        }
      }
    }
    if (null != pool) {
      pool.shutdown();
      for (Future<ObjectPair<Path, Path>> future : futures) {
        try {
          ObjectPair<Path, Path> pair = future.get();
          LOG.debug("Moved src: {}, to dest: {}", pair.getFirst().toString(), pair.getSecond().toString());
        } catch (Exception e) {
          throw handlePoolException(pool, e);
        }
      }
    }
  }

  /**
   * Replaces files in the partition with new data set specified by srcf. Works
   * by renaming directory of srcf to the destination file.
   * srcf, destf, and tmppath should resident in the same DFS, but the oldPath can be in a
   * different DFS.
   *
   * @param tablePath path of the table.  Used to identify permission inheritance.
   * @param srcf
   *          Source directory to be renamed to tmppath. It should be a
   *          leaf directory where the final data files reside. However it
   *          could potentially contain subdirectories as well.
   * @param destf
   *          The directory where the final data needs to go
   * @param oldPath
   *          The directory where the old data location, need to be cleaned up.  Most of time, will be the same
   *          as destf, unless its across FileSystem boundaries.
   * @param purge
   *          When set to true files which needs to be deleted are not moved to Trash
   * @param isSrcLocal
   *          If the source directory is LOCAL
   * @param newFiles
   *          Output the list of new files replaced in the destination path
   * @param isManaged
   *          If the table is managed.
   */
  @Override
  public void replaceFiles(Path tablePath, Path srcf, Path destf, Path oldPath, HiveConf conf,
          boolean isSrcLocal, boolean purge, List<Path> newFiles, PathFilter deletePathFilter,
          boolean isNeedRecycle, boolean isManaged, Hive hive) throws HiveException {
    try {

      FileSystem destFs = destf.getFileSystem(conf);
      // check if srcf contains nested sub-directories
      FileStatus[] srcs;
      FileSystem srcFs;
      try {
        srcFs = srcf.getFileSystem(conf);
        srcs = srcFs.globStatus(srcf);
      } catch (IOException e) {
        throw new HiveException("Getting globStatus " + srcf.toString(), e);
      }
      if (srcs == null) {
        LOG.info("No sources specified to move: " + srcf);
        return;
      }

      if (oldPath != null) {
        deleteOldPathForReplace(destf, oldPath, conf, purge, deletePathFilter, isNeedRecycle, hive);
      }

      // first call FileUtils.mkdir to make sure that destf directory exists, if not, it creates
      // destf
      boolean destfExist = FileUtils.mkdir(destFs, destf, conf);
      if(!destfExist) {
        throw new IOException("Directory " + destf.toString()
            + " does not exist and could not be created.");
      }

      // Two cases:
      // 1. srcs has only a src directory, if rename src directory to destf, we also need to
      // Copy/move each file under the source directory to avoid to delete the destination
      // directory if it is the root of an HDFS encryption zone.
      // 2. srcs must be a list of files -- ensured by LoadSemanticAnalyzer
      // in both cases, we move the file under destf
      if (srcs.length == 1 && srcs[0].isDirectory()) {
        if (!moveFile(conf, srcs[0].getPath(), destf, true, isSrcLocal, isManaged)) {
          throw new IOException("Error moving: " + srcf + " into: " + destf);
        }

        // Add file paths of the files that will be moved to the destination if the caller needs it
        if (null != newFiles) {
          listNewFilesRecursively(destFs, destf, newFiles);
        }
      } else {
        // its either a file or glob
        for (FileStatus src : srcs) {
          Path destFile = new Path(destf, src.getPath().getName());
          if (!moveFile(conf, src.getPath(), destFile, true, isSrcLocal, isManaged)) {
            throw new IOException("Error moving: " + srcf + " into: " + destf);
          }

          // Add file paths of the files that will be moved to the destination if the caller needs it
          if (null != newFiles) {
            newFiles.add(destFile);
          }
        }
      }
    } catch (IOException e) {
      throw new HiveException(e.getMessage(), e);
    }
  }

   public void deleteOldPathForReplace(Path destPath, Path oldPath, HiveConf conf, boolean purge,
      PathFilter pathFilter, boolean isNeedRecycle, Hive hive) throws HiveException {
    Utilities.FILE_OP_LOGGER.debug("Deleting old paths for replace in " + destPath
        + " and old path " + oldPath);
    boolean isOldPathUnderDestf = false;
    try {
      FileSystem oldFs = oldPath.getFileSystem(conf);
      FileSystem destFs = destPath.getFileSystem(conf);
      // if oldPath is destf or its subdir, its should definitely be deleted, otherwise its
      // existing content might result in incorrect (extra) data.
      // But not sure why we changed not to delete the oldPath in HIVE-8750 if it is
      // not the destf or its subdir?
      isOldPathUnderDestf = Hive.isSubDir(oldPath, destPath, oldFs, destFs, false);
      if (isOldPathUnderDestf) {
        cleanUpOneDirectoryForReplace(oldPath, oldFs, pathFilter, conf, purge, isNeedRecycle, hive);
      }
    } catch (IOException e) {
      if (isOldPathUnderDestf) {
        // if oldPath is a subdir of destf but it could not be cleaned
        throw new HiveException("Directory " + oldPath.toString()
            + " could not be cleaned up.", e);
      } else {
        //swallow the exception since it won't affect the final result
        LOG.warn("Directory " + oldPath.toString() + " cannot be cleaned: " + e, e);
      }
    }
  }

  private void cleanUpOneDirectoryForReplace(Path path, FileSystem fs, PathFilter pathFilter,
                                             HiveConf conf, boolean purge, boolean isNeedRecycle,
                                             Hive hive) throws IOException, HiveException {
    if (isNeedRecycle && conf.getBoolVar(HiveConf.ConfVars.REPLCMENABLED)) {
      recycleDirToCmPath(path, purge, hive);
    }
    FileStatus[] statuses = fs.listStatus(path, pathFilter);
    if (statuses == null || statuses.length == 0) {
      return;
    }
    if (Utilities.FILE_OP_LOGGER.isTraceEnabled()) {
      String s = "Deleting files under " + path + " for replace: ";
      for (FileStatus file : statuses) {
        s += file.getPath().getName() + ", ";
      }
      Utilities.FILE_OP_LOGGER.trace(s);
    }

    if (!Hive.trashFiles(fs, statuses, conf, purge)) {
      throw new HiveException("Old path " + path + " has not been cleaned up.");
    }
  }

  /**
   * Recycles the files recursively from the input path to the cmroot directory either by copying or moving it.
   *
   * @param dataPath Path of the data files to be recycled to cmroot
   * @param isPurge
   *          When set to true files which needs to be recycled are not moved to Trash
   */
  public void recycleDirToCmPath(Path dataPath, boolean isPurge, Hive hive) throws HiveException {
    try {
      CmRecycleRequest request = new CmRecycleRequest(dataPath.toString(), isPurge);
      hive.getMSC().recycleDirToCmPath(request);
    } catch (Exception e) {
      throw new HiveException(e);
    }
  }

    /**
   * <p>
   *   Moves a file from one {@link Path} to another. If {@code isRenameAllowed} is true then the
   *   {@link FileSystem#rename(Path, Path)} method is used to move the file. If its false then the data is copied, if
   *   {@code isSrcLocal} is true then the {@link FileSystem#copyFromLocalFile(Path, Path)} method is used, else
   *   {@link FileUtils#copy(FileSystem, Path, FileSystem, Path, boolean, boolean, HiveConf)} is used.
   * </p>
   *
   * <p>
   *   If the destination file already exists, then {@code _copy_[counter]} is appended to the file name, where counter
   *   is an integer starting from 1.
   * </p>
   *
   * @param conf the {@link HiveConf} to use if copying data
   * @param sourceFs the {@link FileSystem} where the source file exists
   * @param sourcePath the {@link Path} to move
   * @param destFs the {@link FileSystem} to move the file to
   * @param destDirPath the {@link Path} to move the file to
   * @param isSrcLocal if the source file is on the local filesystem
   * @param isOverwrite if true, then overwrite destination file if exist else make a duplicate copy
   * @param isRenameAllowed true if the data should be renamed and not copied, false otherwise
   *
   * @return the {@link Path} the source file was moved to
   *
   * @throws IOException if there was an issue moving the file
   */
  private static Path mvFile(HiveConf conf, FileSystem sourceFs, Path sourcePath, FileSystem destFs,
                            Path destDirPath, boolean isSrcLocal, boolean isOverwrite,
                            boolean isRenameAllowed, int taskId) throws IOException {

    // Strip off the file type, if any so we don't make:
    // 000000_0.gz -> 000000_0.gz_copy_1
    final String fullname = sourcePath.getName();
    final String name;
    if (taskId == -1) { // non-acid
      name = FilenameUtils.getBaseName(sourcePath.getName());
    } else { // acid
      name = getPathName(taskId);
    }
    final String type = FilenameUtils.getExtension(sourcePath.getName());

    // Incase of ACID, the file is ORC so the extension is not relevant and should not be inherited.
    Path destFilePath = new Path(destDirPath, taskId == -1 ? fullname : name);

    /*
+    * The below loop may perform bad when the destination file already exists and it has too many _copy_
+    * files as well. A desired approach was to call listFiles() and get a complete list of files from
+    * the destination, and check whether the file exists or not on that list. However, millions of files
+    * could live on the destination directory, and on concurrent situations, this can cause OOM problems.
+    *
+    * I'll leave the below loop for now until a better approach is found.
+    */
    for (int counter = 1; destFs.exists(destFilePath); counter++) {
      if (isOverwrite) {
        destFs.delete(destFilePath, false);
        break;
      }
      destFilePath =  new Path(destDirPath, name + (Utilities.COPY_KEYWORD + counter) +
              ((taskId == -1 && !type.isEmpty()) ? "." + type : ""));
    }

    if (isRenameAllowed) {
      destFs.rename(sourcePath, destFilePath);
    } else if (isSrcLocal) {
      destFs.copyFromLocalFile(sourcePath, destFilePath);
    } else {
      FileUtils.copy(sourceFs, sourcePath, destFs, destFilePath,
          true,   // delete source
          false,  // overwrite destination
          conf);
    }
    return destFilePath;
  }

  /**
   * If moving across different FileSystems or differnent encryption zone, need to do a File copy instead of rename.
   * TODO- consider if need to do this for different file authority.
   * @throws HiveException
   */
  static private boolean needToCopy(Path srcf, Path destf, FileSystem srcFs,
                                      FileSystem destFs, String configuredOwner, boolean isManaged) throws HiveException {
    //Check if different FileSystems
    if (!FileUtils.equalsFileSystem(srcFs, destFs)) {
      return true;
    }

    if (isManaged && !configuredOwner.isEmpty() && srcFs instanceof DistributedFileSystem) {
      // Need some extra checks
      // Get the running owner
      FileStatus srcs;

      try {
        srcs = srcFs.getFileStatus(srcf);
        String runningUser = UserGroupInformation.getLoginUser().getShortUserName();
        boolean isOwned = FileUtils.isOwnerOfFileHierarchy(srcFs, srcs, configuredOwner, false);
        if (configuredOwner.equals(runningUser)) {
          // Check if owner has write permission, else it will have to copy
          if (!(isOwned &&
              FileUtils.isActionPermittedForFileHierarchy(
                  srcFs, srcs, configuredOwner, FsAction.WRITE, false))) {
            return true;
          }
        } else {
          // If the configured owner does not own the file, throw
          if (!isOwned) {
            throw new HiveException("Load Data failed for " + srcf + " as the file is not owned by "
            + configuredOwner + " and load data is also not ran as " + configuredOwner);
          } else {
            return true;
          }
        }
      } catch (IOException e) {
        throw new HiveException("Could not fetch FileStatus for source file");
      } catch (HiveException e) {
        throw new HiveException(e);
      } catch (Exception e) {
        throw new HiveException(" Failed in looking up Permissions on file + " + srcf);
      }
    }

    //Check if different encryption zones
    HadoopShims.HdfsEncryptionShim srcHdfsEncryptionShim = SessionState.get().getHdfsEncryptionShim(srcFs);
    HadoopShims.HdfsEncryptionShim destHdfsEncryptionShim = SessionState.get().getHdfsEncryptionShim(destFs);
    try {
      return srcHdfsEncryptionShim != null
          && destHdfsEncryptionShim != null
          && (srcHdfsEncryptionShim.isPathEncrypted(srcf) || destHdfsEncryptionShim.isPathEncrypted(destf))
          && !srcHdfsEncryptionShim.arePathsOnSameEncryptionZone(srcf, destf, destHdfsEncryptionShim);
    } catch (IOException e) {
      throw new HiveException(e);
    }
  }

  static private HiveException handlePoolException(ExecutorService pool, Exception e) {
    HiveException he = null;

    if (e instanceof HiveException) {
      he = (HiveException) e;
      if (he.getCanonicalErrorMsg() != ErrorMsg.GENERIC_ERROR) {
        if (he.getCanonicalErrorMsg() == ErrorMsg.UNRESOLVED_RT_EXCEPTION) {
          LOG.error("Failed to move: {}", he.getMessage());
        } else {
          LOG.error("Failed to move: {}", he.getRemoteErrorMsg());
        }
      }
    } else {
      LOG.error("Failed to move: {}", e.getMessage());
      he = new HiveException(e.getCause());
    }
    pool.shutdownNow();
    return he;
  }

  // List the new files in destination path which gets copied from source.
  private static void listNewFilesRecursively(final FileSystem destFs, Path dest,
                                             List<Path> newFiles) throws HiveException {
    try {
      for (FileStatus fileStatus : destFs.listStatus(dest, FileUtils.HIDDEN_FILES_PATH_FILTER)) {
        if (fileStatus.isDirectory()) {
          // If it is a sub-directory, then recursively list the files.
          listNewFilesRecursively(destFs, fileStatus.getPath(), newFiles);
        } else {
          newFiles.add(fileStatus.getPath());
        }
      }
    } catch (IOException e) {
      LOG.error("Failed to get source file statuses", e);
      throw new HiveException(e.getMessage(), e);
    }
  }

  private static String getPathName(int taskId) {
    return Utilities.replaceTaskId("000000", taskId) + "_0";
  }
}