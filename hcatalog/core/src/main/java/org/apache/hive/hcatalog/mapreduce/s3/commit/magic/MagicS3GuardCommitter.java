package org.apache.hive.hcatalog.mapreduce.s3.commit.magic;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitterFactory;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
/**
 * This is a dedicated committer which requires the "magic" directory feature
 * of the S3A Filesystem to be enabled; it then uses paths for task and job
 * attempts in magic paths, so as to ensure that the final output goes direct
 * to the destination directory.
 */
@InterfaceAudience.Public
@InterfaceStability.Unstable
public class MagicS3GuardCommitter extends OutputCommitter {

    org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter committer = null;
    public MagicS3GuardCommitter() {
    } //necessary only for mapred API reasons, should never be actually used

    public MagicS3GuardCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
        committer = (org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter) MagicS3GuardCommitterFactory.createCommitter(outputPath, context);
    }

    public MagicS3GuardCommitter(Path outputPath, JobContext context) throws IOException {
        TaskAttemptContext tac = new org.apache.hadoop.mapred.TaskAttemptContextImpl(context.getJobConf(), new TaskAttemptID(context.getJobID().getJtIdentifier(), context.getJobID().getId(), false,0,0));
        committer = (org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter) MagicS3GuardCommitterFactory.createCommitter(outputPath, tac);
    }

    private org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter getWrapped(JobContext context) throws IOException {
        //Hadoop's committer only supports being created with a TaskAttemptContext, so we create a dummy instance for it since we only care about the job in this case
        TaskAttemptContext tac = new org.apache.hadoop.mapred.TaskAttemptContextImpl(context.getJobConf(), new TaskAttemptID(context.getJobID().getJtIdentifier(), context.getJobID().getId(), false,0,0));
        if (committer == null) {
            committer = (org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter) MagicS3GuardCommitterFactory.createCommitter(new Path(context.getConfiguration().get("mapred.output.dir")), tac);
        }
        return committer;
    }

    private org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter getWrapped(TaskAttemptContext context) throws IOException {
        if (committer == null) {
            committer = (org.apache.hadoop.fs.s3a.commit.magic.MagicS3GuardCommitter) MagicS3GuardCommitterFactory.createCommitter(new Path(context.getConfiguration().get("mapred.output.dir")), context);
        }
        return committer;
    }

    @Override
    public void setupJob(JobContext context) throws IOException {
        getWrapped(context).setupJob(context);
    }

    @Override
    public void setupTask(org.apache.hadoop.mapred.TaskAttemptContext context) throws IOException {
        getWrapped(context).setupTask(context);
    }

    @Override
    public boolean needsTaskCommit(org.apache.hadoop.mapred.TaskAttemptContext context) throws IOException {
        return getWrapped(context).needsTaskCommit(context);
    }

    @Override
    public void commitTask(org.apache.hadoop.mapred.TaskAttemptContext context) throws IOException {
        getWrapped(context).commitTask(context);
    }

    @Override
    public void abortTask(org.apache.hadoop.mapred.TaskAttemptContext context) throws IOException {
        getWrapped(context).abortTask(context);
    }

    public void commitJob(org.apache.hadoop.mapred.JobContext context) throws IOException {
        getWrapped(context).commitJob(context);
    }

    public final Path getWorkPath() {
        return committer.getWorkPath();
    }
}

