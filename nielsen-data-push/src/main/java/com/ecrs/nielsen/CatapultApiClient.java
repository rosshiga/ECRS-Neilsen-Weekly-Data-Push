package com.ecrs.nielsen;

import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.net.URIBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * HTTP client for interacting with the Catapult API.
 * Fetches summary item data in CSV format.
 */
public class CatapultApiClient {

    private static final String API_BASE_URL_TEMPLATE = "https://%s.catapultweboffice.com/api";
    private static final DateTimeFormatter API_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");

    private final String accountId;
    private final String apiKey;

    /**
     * Creates a new Catapult API client.
     *
     * @param accountId The Catapult account ID (used as subdomain)
     * @param apiKey    The API key for authentication
     */
    public CatapultApiClient(String accountId, String apiKey) {
        this.accountId = accountId;
        this.apiKey = apiKey;
    }

    /**
     * Fetches summary item data from the Catapult API.
     *
     * @param store     The store number (e.g., "RS1")
     * @param startDate The start date for the data range
     * @param endDate   The end date for the data range
     * @return CSV data as a string
     * @throws IOException if the API request fails
     */
    public String fetchSummaryItemData(String store, LocalDateTime startDate, LocalDateTime endDate) throws IOException {
        String baseUrl = String.format(API_BASE_URL_TEMPLATE, accountId);

        try {
            URI uri = new URIBuilder(baseUrl + "/summaryItemData")
                    .addParameter("apikey", apiKey)
                    .addParameter("startDate", startDate.format(API_DATE_FORMAT))
                    .addParameter("endDate", endDate.format(API_DATE_FORMAT))
                    .addParameter("stores", store)
                    .addParameter("Type", "1")
                    .build();

            HttpGet request = new HttpGet(uri);
            request.addHeader("X-ECRS-APIKEY", apiKey);
            request.addHeader("Accept", "text/csv");

            try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
                return httpClient.execute(request, response -> {
                    int statusCode = response.getCode();
                    if (statusCode != 200) {
                        String body = EntityUtils.toString(response.getEntity());
                        throw new IOException("API request failed with status " + statusCode + ": " + body);
                    }
                    return EntityUtils.toString(response.getEntity());
                });
            }

        } catch (URISyntaxException e) {
            throw new IOException("Failed to build API URL: " + e.getMessage(), e);
        }
    }
}

