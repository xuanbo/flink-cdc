package tk.fishfish.cdc.util;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;
import tk.fishfish.cdc.config.Job;

import java.io.FileInputStream;
import java.io.IOException;

/**
 * 任务工具
 *
 * @author 奔波儿灞
 * @version 1.0.0
 */
public final class JobUtils {

    private JobUtils() {
        throw new IllegalStateException("Utils");
    }

    public static void valid(Job job) {
        if (job == null) {
            throw new IllegalArgumentException("配置 [job] 不能为空");
        }
        if (StringUtils.isBlank(job.getName())) {
            throw new IllegalArgumentException("配置 [job.name] 不能为空");
        }
        if (job.getParallelism() == null) {
            throw new IllegalArgumentException("配置 [job.parallelism] 不能为空");
        }
        if (job.getParallelism() <= 0) {
            throw new IllegalArgumentException("配置 [job.parallelism] 必须大于0");
        }
        if (job.getCheckpoint() == null) {
            job.setCheckpoint(new Job.Checkpoint());
        }
        if (StringUtils.isBlank(job.getCheckpoint().getDir())) {
            throw new IllegalArgumentException("配置 [job.checkpoint.dir] 不能为空");
        }
        if (job.getCheckpoint().getInterval() == null) {
            throw new IllegalArgumentException("配置 [job.checkpoint.interval] 不能为空");
        }
        if (job.getCheckpoint().getInterval() <= 0) {
            throw new IllegalArgumentException("配置 [job.checkpoint.interval] 必须大于0");
        }
        if (job.getCheckpoint().getTimeout() == null) {
            throw new IllegalArgumentException("配置 [job.checkpoint.timeout] 不能为空");
        }
        if (job.getCheckpoint().getTimeout() <= 0) {
            throw new IllegalArgumentException("配置 [job.checkpoint.timeout] 必须大于0");
        }
        if (job.getCheckpoint().getMinPauseBetweenCheckpoints() == null) {
            throw new IllegalArgumentException("配置 [job.checkpoint.minPauseBetweenCheckpoints] 不能为空");
        }
        if (job.getCheckpoint().getMinPauseBetweenCheckpoints() <= 0) {
            throw new IllegalArgumentException("配置 [job.checkpoint.minPauseBetweenCheckpoints] 必须大于0");
        }
        if (job.getCheckpoint().getMaxConcurrentCheckpoints() == null) {
            throw new IllegalArgumentException("配置 [job.checkpoint.maxConcurrentCheckpoints] 不能为空");
        }
        if (job.getCheckpoint().getMaxConcurrentCheckpoints() <= 0) {
            throw new IllegalArgumentException("配置 [job.checkpoint.maxConcurrentCheckpoints] 必须大于0");
        }
        if (job.getSavepoint() == null) {
            job.setSavepoint(new Job.Savepoint());
        }
        if (job.getSavepoint().getEnabled() == null) {
            job.getSavepoint().setEnabled(false);
        }
        if (CollectionUtils.isEmpty(job.getPipeline())) {
            throw new IllegalArgumentException("配置 [job.pipeline] 不能为空");
        }
    }

    public static Job read(String jobPath) {
        Job job;
        if (StringUtils.endsWith(jobPath, ".yaml") || StringUtils.endsWith(jobPath, ".yml")) {
            try (FileInputStream in = new FileInputStream(jobPath)) {
                job = new Yaml().loadAs(in, Job.class);
            } catch (IOException e) {
                throw new YAMLException("读取配置文件错误", e);
            }
        } else if (StringUtils.endsWith(jobPath, ".json")) {
            try (FileInputStream in = new FileInputStream(jobPath)) {
                ObjectMapper om = new ObjectMapper();
                job = om.readValue(in, Job.class);
            } catch (IOException e) {
                throw new RuntimeException("读取配置文件错误", e);
            }
        } else {
            throw new IllegalArgumentException("不支持的配置文件: " + jobPath);
        }
        return job;
    }

}
