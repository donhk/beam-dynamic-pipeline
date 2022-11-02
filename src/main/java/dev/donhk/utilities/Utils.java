package dev.donhk.utilities;


import dev.donhk.pojos.CarInformation;
import dev.donhk.pojos.Comment;
import dev.donhk.pojos.Dag;
import dev.donhk.pojos.UserTxn;
import dev.donhk.transform.JoinType;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.yaml.snakeyaml.Yaml;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
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

    public static List<UserTxn> getUserTxnList() {
        final String[] HEADERS = {"id", "first_name", "last_name", "email", "gender", "time", "amount"};
        final List<UserTxn> list = new ArrayList<>();
        DateTimeFormatter formatter =
                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        try (Reader in = new FileReader("assets/user_txn_list.csv")) {
            final Iterable<CSVRecord> records = CSVFormat.DEFAULT.builder()
                    .setHeader(HEADERS)
                    .setSkipHeaderRecord(true)
                    .build()
                    .parse(in);
            for (CSVRecord record : records) {
                final LocalDateTime time =
                        Instant.ofEpochMilli(Long.parseLong(record.get("time")))
                                .atZone(ZoneId.systemDefault())
                                .toLocalDateTime();
                final long id = Integer.parseInt(record.get("id"));
                final String amountStr = record.get("amount").replace("$", "");
                list.add(new UserTxn(
                        id,
                        record.get("first_name"),
                        record.get("last_name"),
                        record.get("email"),
                        record.get("gender"),
                        time,
                        Double.parseDouble(amountStr)
                ));
            }
            return list;
        } catch (IOException io) {
            LOG.error(io);
            return Collections.emptyList();
        }
    }

    public static List<Comment> getCommentList() {
        final String[] HEADERS = {"id", "username", "comment", "background"};
        return null;
    }

    public static List<CarInformation> getCarInfoList() {
        final String[] HEADERS = {"id", "car_model", "car_make", "city"};
        return null;
    }
}
