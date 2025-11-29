package com.ecrs.nielsen;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;

/**
 * Processes CSV data from the Catapult API.
 * Filters rows based on omitFromSales and renames/reorders columns for Nielsen.
 */
public class CsvProcessor {

    // Input column names from Catapult API
    private static final String COL_ITEM_ID = "summaryItemID";
    private static final String COL_DESCRIPTION = "summaryItemDescription";
    private static final String COL_QTY_SOLD = "summaryQtySold";
    private static final String COL_NET_SALES = "summaryNetSales";
    private static final String COL_OMIT_FROM_SALES = "omitFromSales";

    // Output column names for Nielsen
    private static final String[] OUTPUT_HEADERS = {"Item ID", "Receipt Alias", "Quantity", "Sales"};

    /**
     * Processes the CSV data from the Catapult API.
     * - Filters out rows where omitFromSales != 0
     * - Selects and renames columns for Nielsen format
     *
     * @param csvData The raw CSV data from the API
     * @return Processed CSV data as a string
     * @throws IOException if CSV parsing fails
     */
    public String process(String csvData) throws IOException {
        StringWriter output = new StringWriter();

        CSVFormat inputFormat = CSVFormat.DEFAULT.builder()
                .setHeader()
                .setSkipHeaderRecord(true)
                .setQuote('\'')
                .setTrim(true)
                .build();

        CSVFormat outputFormat = CSVFormat.DEFAULT.builder()
                .setHeader(OUTPUT_HEADERS)
                .build();

        try (CSVParser parser = CSVParser.parse(csvData, inputFormat);
             CSVPrinter printer = new CSVPrinter(output, outputFormat)) {

            int totalRows = 0;
            int filteredRows = 0;

            for (CSVRecord record : parser) {
                totalRows++;

                // Get omitFromSales value
                String omitFromSalesStr = record.get(COL_OMIT_FROM_SALES);
                int omitFromSales;
                try {
                    omitFromSales = Integer.parseInt(omitFromSalesStr.trim());
                } catch (NumberFormatException e) {
                    // If parsing fails, treat as 0 (keep the row)
                    omitFromSales = 0;
                }

                // Keep rows where omitFromSales = 0
                if (omitFromSales == 0) {
                    String itemId = record.get(COL_ITEM_ID);
                    String description = record.get(COL_DESCRIPTION);
                    String qtySold = record.get(COL_QTY_SOLD);
                    String netSales = record.get(COL_NET_SALES);

                    // Format quantity: truncate to 2 decimal places
                    String formattedQty = truncateToTwoDecimals(qtySold);

                    // Format sales: truncate to 2 decimal places with dollar sign
                    String formattedSales = "$" + truncateToTwoDecimals(netSales);

                    printer.printRecord(itemId, description, formattedQty, formattedSales);
                    filteredRows++;
                }
            }

            System.out.println("  Total rows read: " + totalRows);
            System.out.println("  Rows after filtering (omitFromSales=0): " + filteredRows);
        }

        return output.toString();
    }

    /**
     * Truncates a numeric string to exactly 2 decimal places (no rounding).
     *
     * @param value The numeric string value
     * @return Formatted string with exactly 2 decimal places
     */
    private String truncateToTwoDecimals(String value) {
        try {
            BigDecimal bd = new BigDecimal(value.trim());
            bd = bd.setScale(2, RoundingMode.DOWN);
            return bd.toPlainString();
        } catch (NumberFormatException e) {
            // If parsing fails, return original value
            return value;
        }
    }

    /**
     * Saves the processed CSV data to a file.
     *
     * @param csvData The processed CSV data
     * @param file    The target file
     * @throws IOException if file writing fails
     */
    public void saveToFile(String csvData, File file) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(
                new OutputStreamWriter(new FileOutputStream(file), StandardCharsets.UTF_8))) {
            writer.write(csvData);
        }
    }
}

