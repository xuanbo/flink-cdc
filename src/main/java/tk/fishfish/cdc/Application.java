package tk.fishfish.cdc;

import org.apache.commons.lang3.BooleanUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tk.fishfish.cdc.config.Job;
import tk.fishfish.cdc.util.JobUtils;
import tk.fishfish.cdc.util.SavepointRestoreUtils;

import java.nio.file.Paths;
import java.util.List;

/**
 * 启动
 *
 * @author 奔波儿灞
 * @version 1.0.0
 */
public class Application {

    private static final Logger LOGGER = LoggerFactory.getLogger(Application.class);

    public static void main(String[] args) {
        if (args.length == 0) {
            throw new IllegalArgumentException("任务需要指定配置文件路径");
        }
        String jobPath = args[0];
        LOGGER.info("配置文件路径: {}", jobPath);

        Job job = JobUtils.read(jobPath);
        JobUtils.valid(job);

        Job.Checkpoint checkpoint = job.getCheckpoint();
        String checkpointDir = Paths.get(checkpoint.getDir(), job.getName()).toString();

        // 获取最新的 checkpoint 恢复任务
        if (BooleanUtils.toBoolean(job.getSavepoint().getEnabled())) {
            String savepointDir = SavepointRestoreUtils.getSavepointRestore(checkpointDir);
            if (savepointDir != null) {
                LOGGER.info("恢复任务: {}", savepointDir);
                System.setProperty("SAVEPOINT_DIR", "file://" + savepointDir);
            }
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(job.getParallelism());
        CheckpointConfig config = env.getCheckpointConfig();
        // 设置checkpoint的存储路径
        env.getCheckpointConfig().setCheckpointStorage("file://" + checkpointDir);
        // 任务流取消和故障时会保留Checkpoint数据，以便根据实际需要恢复到指定的Checkpoint
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        // 设置checkpoint的周期, 每隔 60_000 ms进行启动一个检查点
        config.setCheckpointInterval(checkpoint.getInterval());
        // 设置模式为exactly-once
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 确保检查点之间有至少 5000ms 的间隔【checkpoint最小间隔】
        config.setMinPauseBetweenCheckpoints(checkpoint.getMinPauseBetweenCheckpoints());
        // 检查点必须在 10 分钟内完成，或者被丢弃【checkpoint的超时时间】
        config.setCheckpointTimeout(checkpoint.getTimeout());
        // 同一时间只允许进行一个检查点
        config.setMaxConcurrentCheckpoints(checkpoint.getMaxConcurrentCheckpoints());

        EnvironmentSettings streamSettings = EnvironmentSettings.newInstance()
                .inStreamingMode().useBlinkPlanner().build();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, streamSettings);
        tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);

        LOGGER.info("启动CDC同步任务: {}", job.getName());

        List<String> pipeline = job.getPipeline();
        for (String sql : pipeline) {
            LOGGER.info("提交SQL: {}", sql);
            tableEnv.executeSql(sql);
        }
    }

}
