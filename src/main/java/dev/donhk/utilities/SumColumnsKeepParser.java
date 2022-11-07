package dev.donhk.utilities;

import java.util.List;
import java.util.Locale;

public class SumColumnsKeepParser {
    private final String outputCol;
    private final List<String> columns;

    public SumColumnsKeepParser(String rawCommand) {
        // SumColumnsParser[col1,col2,col3],col4
        final String[] parts = rawCommand.split("],");
        final String cols = parts[0]
                .replace("SumColumnsKeep[", "")
                .replace("]", "")
                .toUpperCase(Locale.ENGLISH);;
        final String[] colNames = cols.split(",");
        this.columns = List.of(colNames);
        this.outputCol = parts[1].replace(",", "");
    }

    public List<String> columnNames() {
        return columns;
    }

    public String outputCol() {
        return outputCol.toUpperCase(Locale.ENGLISH);
    }

}
