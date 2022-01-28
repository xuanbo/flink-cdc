package tk.fishfish.cdc.util;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.Arrays;
import java.util.Comparator;

/**
 * 检查点恢复
 *
 * @author 奔波儿灞
 * @version 1.0.0
 */
public final class SavepointRestoreUtils {

    private SavepointRestoreUtils() {
        throw new IllegalStateException("Utils");
    }

    public static String getSavepointRestore(String checkpointDir) {
        if (StringUtils.startsWith(checkpointDir, "hdfs://")) {
            return null;
        }
        File file = new File(checkpointDir);
        if (!file.exists() || !file.isDirectory()) {
            return null;
        }
        File[] files = file.listFiles();
        if (files == null || files.length == 0) {
            return null;
        }
        file = Arrays.stream(files)
                .filter(File::isDirectory)
                .max(Comparator.comparing(File::lastModified))
                .orElse(null);
        if (file == null) {
            return null;
        }
        files = file.listFiles();
        if (files == null || files.length == 0) {
            return null;
        }
        file = Arrays.stream(files)
                .filter(File::isDirectory)
                .filter(f -> StringUtils.startsWith(f.getName(), "chk-"))
                .max(Comparator.comparing(File::lastModified))
                .orElse(null);
        if (file == null) {
            return null;
        }
        return file.getPath();
    }

}
