package com.ecrs.nielsen;

import org.apache.commons.cli.*;

import java.io.File;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;

/**
 * Main entry point for the Nielsen Data Push application.
 * Fetches item movement data from Catapult API, processes it, and uploads to Nielsen via SFTP.
 */
public class NielsenDataPush {

    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter API_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    public static void main(String[] args) {
        Options options = createOptions();
        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        try {
            CommandLine cmd = parser.parse(options, args);

            // Extract required arguments
            String accountId = cmd.getOptionValue("accountId");
            String apiKey = cmd.getOptionValue("apiKey");
            String store = cmd.getOptionValue("store");
            String nielsenName = cmd.getOptionValue("nielsenName");
            String sftpHost = cmd.getOptionValue("sftpHost");
            int sftpPort = Integer.parseInt(cmd.getOptionValue("sftpPort"));
            String sftpUser = cmd.getOptionValue("sftpUser");
            String sftpPassword = cmd.getOptionValue("sftpPassword");
            String sftpPath = cmd.getOptionValue("sftpPath");

            // Determine date range
            LocalDateTime startDate;
            LocalDateTime endDate;

            if (cmd.hasOption("days")) {
                int days = Integer.parseInt(cmd.getOptionValue("days"));
                LocalDate yesterday = LocalDate.now().minusDays(1);
                endDate = yesterday.atTime(23, 59, 59, 999999000);
                startDate = yesterday.minusDays(days - 1).atStartOfDay();
            } else if (cmd.hasOption("startDate") && cmd.hasOption("endDate")) {
                try {
                    LocalDate start = LocalDate.parse(cmd.getOptionValue("startDate"), DATE_FORMAT);
                    LocalDate end = LocalDate.parse(cmd.getOptionValue("endDate"), DATE_FORMAT);
                    startDate = start.atStartOfDay();
                    endDate = end.atTime(23, 59, 59, 999999000);
                } catch (DateTimeParseException e) {
                    System.err.println("Invalid date format. Use yyyy-MM-dd (e.g., 2025-10-31)");
                    System.exit(1);
                    return;
                }
            } else {
                System.err.println("Error: You must specify either --days or both --startDate and --endDate");
                formatter.printHelp("nielsen-data-push", options);
                System.exit(1);
                return;
            }

            System.out.println("=== Nielsen Data Push ===");
            System.out.println("Account ID: " + accountId);
            System.out.println("Store: " + store);
            System.out.println("Nielsen Name: " + nielsenName);
            System.out.println("Date Range: " + startDate.format(API_DATE_FORMAT) + " to " + endDate.format(API_DATE_FORMAT));
            System.out.println();

            // Step 1: Fetch data from Catapult API
            System.out.println("Fetching data from Catapult API...");
            CatapultApiClient apiClient = new CatapultApiClient(accountId, apiKey);
            String csvData = apiClient.fetchSummaryItemData(store, startDate, endDate);
            System.out.println("Data fetched successfully.");

            // Step 2: Process CSV data
            System.out.println("Processing CSV data...");
            CsvProcessor processor = new CsvProcessor();
            String processedCsv = processor.process(csvData);
            System.out.println("CSV processing complete.");

            // Step 3: Save to local file
            String fileName = nielsenName + ".csv";
            File tempFile = new File(System.getProperty("java.io.tmpdir"), fileName);
            processor.saveToFile(processedCsv, tempFile);
            System.out.println("Saved to temporary file: " + tempFile.getAbsolutePath());

            // Step 4: Upload via SFTP
            System.out.println("Uploading to Nielsen SFTP server...");
            SftpUploader uploader = new SftpUploader(sftpHost, sftpPort, sftpUser, sftpPassword);
            String remotePath = sftpPath + "/" + fileName;
            uploader.upload(tempFile, remotePath);
            System.out.println("Upload complete: " + remotePath);

            // Cleanup
            if (tempFile.delete()) {
                System.out.println("Temporary file cleaned up.");
            }

            System.out.println();
            System.out.println("=== Process Complete ===");

        } catch (ParseException e) {
            System.err.println("Error parsing command line arguments: " + e.getMessage());
            formatter.printHelp("nielsen-data-push", options);
            System.exit(1);
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static Options createOptions() {
        Options options = new Options();

        // Catapult API arguments
        options.addOption(Option.builder()
                .longOpt("accountId")
                .desc("Catapult account ID (API subdomain)")
                .hasArg()
                .required()
                .build());

        options.addOption(Option.builder()
                .longOpt("apiKey")
                .desc("Catapult API key")
                .hasArg()
                .required()
                .build());

        options.addOption(Option.builder()
                .longOpt("store")
                .desc("Store number (e.g., RS1)")
                .hasArg()
                .required()
                .build());

        options.addOption(Option.builder()
                .longOpt("nielsenName")
                .desc("Nielsen output name (used for filename)")
                .hasArg()
                .required()
                .build());

        // Date range arguments
        options.addOption(Option.builder()
                .longOpt("startDate")
                .desc("Start date (yyyy-MM-dd)")
                .hasArg()
                .build());

        options.addOption(Option.builder()
                .longOpt("endDate")
                .desc("End date (yyyy-MM-dd)")
                .hasArg()
                .build());

        options.addOption(Option.builder()
                .longOpt("days")
                .desc("Number of past days (alternative to startDate/endDate)")
                .hasArg()
                .build());

        // SFTP arguments
        options.addOption(Option.builder()
                .longOpt("sftpHost")
                .desc("Nielsen SFTP host")
                .hasArg()
                .required()
                .build());

        options.addOption(Option.builder()
                .longOpt("sftpPort")
                .desc("Nielsen SFTP port")
                .hasArg()
                .required()
                .build());

        options.addOption(Option.builder()
                .longOpt("sftpUser")
                .desc("Nielsen SFTP username")
                .hasArg()
                .required()
                .build());

        options.addOption(Option.builder()
                .longOpt("sftpPassword")
                .desc("Nielsen SFTP password")
                .hasArg()
                .required()
                .build());

        options.addOption(Option.builder()
                .longOpt("sftpPath")
                .desc("Nielsen SFTP remote path")
                .hasArg()
                .required()
                .build());

        return options;
    }
}

