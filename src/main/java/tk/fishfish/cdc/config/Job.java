package tk.fishfish.cdc.config;

import lombok.Data;

import java.util.List;

/**
 * 任务
 *
 * @author 奔波儿灞
 * @version 1.0.0
 */
@Data
public class Job {

    private String name;

    private Integer parallelism = 1;

    private Checkpoint checkpoint = new Checkpoint();

    private Savepoint savepoint = new Savepoint();

    private List<String> pipeline;

    @Data
    public static class Checkpoint {

        private String dir;

        private Long interval = 60_000L;

        private Long minPauseBetweenCheckpoints = 5000L;

        private Long timeout = 600_000L;

        private Integer maxConcurrentCheckpoints = 1;

    }

    @Data
    public static class Savepoint {

        private Boolean enabled = true;

    }

}
