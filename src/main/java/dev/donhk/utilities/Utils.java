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

    public static Dag getElasticDag() {
        return parseDagFile("assets/dag-s1.yaml");
    }

    public static List<Dag> getDags() {
        final List<String> files = Arrays.asList(
                "assets/dag-1.yaml",
                "assets/dag-2.yaml",
                "assets/dag-3.yaml"
        );
        final List<Dag> actualDags = new ArrayList<>();
        for (String file : files) {
            actualDags.add(parseDagFile(file));
        }
        return actualDags;
    }

    @SuppressWarnings("unchecked")
    private static Dag parseDagFile(String file) {
        final Yaml yaml = new Yaml();

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
        return newDag;
    }

    public static List<UserTxn> getUserTxnList() {
        //id,first_name,last_name,email,gender,time,amount,match,memory
        final String[] HEADERS = {"id", "first_name", "last_name", "email", "gender", "time", "amount", "match", "memory"};
        final List<UserTxn> list = new ArrayList<>();
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
                final long id = Long.parseLong(record.get("id"));
                final String amountStr = record.get("amount").replace("$", "");
                final String match = record.get("match");
                final String memory = record.get("memory");
                list.add(new UserTxn(
                        id,
                        record.get("first_name"),
                        record.get("last_name"),
                        record.get("email"),
                        record.get("gender"),
                        time,
                        Double.parseDouble(amountStr),
                        Double.parseDouble(match),
                        Double.parseDouble(memory)
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
        final List<Comment> list = new ArrayList<>();
        try (Reader in = new FileReader("assets/comments.csv")) {
            final Iterable<CSVRecord> records = CSVFormat.DEFAULT.builder()
                    .setHeader(HEADERS)
                    .setSkipHeaderRecord(true)
                    .build()
                    .parse(in);
            for (CSVRecord record : records) {
                final long id = Long.parseLong(record.get("id"));
                list.add(new Comment(
                        id,
                        record.get("username"),
                        record.get("comment"),
                        record.get("background")
                ));
            }
            return list;
        } catch (IOException io) {
            LOG.error(io);
            return Collections.emptyList();
        }
    }

    public static List<CarInformation> getCarInfoList() {
        final String[] HEADERS = {"id", "car_model", "car_make", "city", "car_time", "cost", "promo"};
        final List<CarInformation> list = new ArrayList<>();
        try (Reader in = new FileReader("assets/car_info.csv")) {
            final Iterable<CSVRecord> records = CSVFormat.DEFAULT.builder()
                    .setHeader(HEADERS)
                    .setSkipHeaderRecord(true)
                    .build()
                    .parse(in);
            for (CSVRecord record : records) {
                final long id = Long.parseLong(record.get("id"));
                final LocalDateTime time =
                        Instant.ofEpochMilli(Long.parseLong(record.get("car_time")))
                                .atZone(ZoneId.systemDefault())
                                .toLocalDateTime();
                list.add(new CarInformation(
                        id,
                        record.get("car_model"),
                        record.get("car_make"),
                        record.get("city"),
                        time,
                        Double.parseDouble(record.get("cost")),
                        Double.parseDouble(record.get("promo"))
                ));
            }
            return list;
        } catch (IOException io) {
            LOG.error(io);
            return Collections.emptyList();
        }
    }
}
