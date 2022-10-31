package dev.donhk.utilities;


import dev.donhk.transform.JoinType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

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

    @SuppressWarnings("unchecked")
    public static List<Dag> getDags() {
        final List<String> files = Arrays.asList(
                "assets/dag-1.yaml",
                "assets/dag-2.yaml",
                "assets/dag-3.yaml"
        );
        final Yaml yaml = new Yaml();
        final List<Dag> actualDags = new ArrayList<>();
        for (String file : files) {
            final Map<String, Object> obj;
            try {
                obj = yaml.load(new FileReader(file));
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
            final Dag newDag = new Dag();

            final String joinStr = (String) obj.get("join");

            newDag.setTransforms((ArrayList<String>) obj.get("transforms"));
            newDag.setJoin(JoinType.valueOf(joinStr));
            newDag.setFilter((String) obj.get("filters"));
            newDag.setTop((int) obj.get("top"));
            newDag.setName(file);
            actualDags.add(newDag);
        }
        return actualDags;
    }
}
