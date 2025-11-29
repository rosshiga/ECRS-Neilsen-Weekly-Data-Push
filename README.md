# Nielsen Data Push

Java application that fetches item movement data from the Catapult API, processes it into Nielsen format, and uploads it via SFTP to the Nielsen system.

## Requirements

- Windows OS
- OpenJDK Runtime Environment Temurin
- Maven 3.6 or higher

## Building

```cmd
cd nielsen-data-push
mvn clean package
```

This creates an executable JAR with all dependencies at:
`target\nielsen-data-push-1.0.0.jar`

## Usage

### Command Line Arguments

| Argument | Required | Description |
|----------|----------|-------------|
| `--accountId` | Yes | Catapult account ID (API subdomain, e.g., `25ee1`) |
| `--apiKey` | Yes | Catapult API key |
| `--store` | Yes | Store number (e.g., `RS1`) |
| `--nielsenName` | Yes | Nielsen output name (used for filename, e.g., `WAIANAE`) |
| `--sftpHost` | Yes | Nielsen SFTP host |
| `--sftpPort` | Yes | Nielsen SFTP port |
| `--sftpUser` | Yes | Nielsen SFTP username |
| `--sftpPassword` | Yes | Nielsen SFTP password |
| `--sftpPath` | Yes | Nielsen SFTP remote path |
| `--startDate` | * | Start date (yyyy-MM-dd) |
| `--endDate` | * | End date (yyyy-MM-dd) |
| `--days` | * | Number of past days (alternative to startDate/endDate) |

\* Either `--startDate` and `--endDate` OR `--days` must be provided.

### Date Range Behavior

**Using `--startDate` and `--endDate`:**
- Both dates are **fully inclusive**
- Start date begins at 00:00:00 (midnight)
- End date ends at 23:59:59 (end of day)
- Example: `--startDate 2025-10-31 --endDate 2025-11-07` includes all data from Oct 31 through Nov 7

**Using `--days`:**
- Counts backward from **yesterday** (today is never included since it's incomplete)
- Example: If today is 11/28 and you use `--days 7`:
  - End date: 11/27 (yesterday)
  - Start date: 11/21
  - Result: 7 full days from 11/21 through 11/27

### Examples

**Using specific date range:**

```cmd
java -jar target\nielsen-data-push-1.0.0.jar ^
  --accountId 25ee1 ^
  --apiKey "YOUR_API_KEY" ^
  --store RS1 ^
  --nielsenName WAIANAE ^
  --startDate 2025-10-31 ^
  --endDate 2025-11-07 ^
  --sftpHost namft.nielseniq.com ^
  --sftpPort 46422 ^
  --sftpUser "Nielsen@nanukuli_WAM.com" ^
  --sftpPassword "YOUR_PASSWORD" ^
  --sftpPath Nielsen_wam0000
```

**Using number of past days (last 7 days ending yesterday):**

```cmd
java -jar target\nielsen-data-push-1.0.0.jar ^
  --accountId 25ee1 ^
  --apiKey "YOUR_API_KEY" ^
  --store RS1 ^
  --nielsenName WAIANAE ^
  --days 7 ^
  --sftpHost namft.nielseniq.com ^
  --sftpPort 46422 ^
  --sftpUser "Nielsen@nanukuli_WAM.com" ^
  --sftpPassword "YOUR_PASSWORD" ^
  --sftpPath Nielsen_wam0000
```

## Data Processing

The application performs the following transformations on the Catapult API data:

1. **Filters** rows where `omitFromSales = 0` (keeps items that should be included in sales)
2. **Selects and renames** columns:
   - `summaryItemID` → `Item ID`
   - `summaryItemDescription` → `Receipt Alias`
   - `summaryQtySold` → `Quantity`
   - `summaryNetSales` → `Sales`

## Output

The processed CSV file is uploaded to the Nielsen SFTP server at:
`{sftpPath}/{nielsenName}.csv`

For example: `Nielsen_wam0000/WAIANAE.csv`

