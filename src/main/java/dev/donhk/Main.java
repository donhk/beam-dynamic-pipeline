package dev.donhk;

import dev.donhk.core.PipelineBuilder;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Main {

    private static final Logger LOG = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        if (args.length != 2) {
            LOG.error("arg0 = input file\narg1 = dag");
            return;
        }
        LOG.info("arg0 [{}] arg1 [{}]", args[0], args[1]);
        PipelineBuilder builder = new PipelineBuilder(args[0], args[1]);
        builder.execute();
    }
}
