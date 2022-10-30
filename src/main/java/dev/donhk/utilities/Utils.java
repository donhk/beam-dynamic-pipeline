package dev.donhk.utilities;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;

public class Utils {

    private static final Logger LOG = LogManager.getLogger(Utils.class);

    private Utils() {
    }

    public static List<String> readFile(String file) {
        try {
            LOG.info("file " + file);
            return Files.readAllLines(Paths.get(file));
        } catch (IOException e) {
            LOG.error(e.getMessage(), e);
            return Collections.emptyList();
        }
    }
}
