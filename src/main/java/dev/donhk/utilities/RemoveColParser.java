package dev.donhk.utilities;

import java.util.Locale;

public class RemoveColParser {

    private final String colName;

    public RemoveColParser(String transformation) {
        colName = transformation.toUpperCase(Locale.ENGLISH)
                .replace("REMOVECOL", "")
                .replace("[", "")
                .replace("]", "");
    }

    public String colName() {
        return colName;
    }
}
