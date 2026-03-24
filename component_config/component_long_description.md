# Oracle Hyperion EPM Writer

Writes data from Keboola Storage into Oracle Hyperion Enterprise Performance Management (EPM).

## Supported Modules

### Essbase 21C
- Imports CSV table data into Essbase OLAP cubes via the Essbase 21C REST API (port 9010, Basic Auth)
- CSV column headers must match Hyperion dimension names in the cube outline
- Supports any application/database combination

### Hyperion Planning
- Imports data into Planning applications via the Planning REST API (port 9000)
- Authenticates using the CSS SSO token flow

## Prerequisites

- Oracle Hyperion EPM server with Essbase 21C deployed (on-premise or cloud VM)
- Network access from Keboola to the Essbase REST API port (default 9010) and/or Planning port (9000)
- Valid Shared Services (CSS) credentials

## Configuration

Connection settings are configured at the component level (shared across all rows). Each configuration row defines one write operation — module type, application, target cube or plan type, and the input table name from Keboola Storage.

## Column Mapping

CSV column headers are sent directly as Hyperion dimension names in the import payload. Ensure the column names in your input table exactly match the dimension names in the Hyperion outline.
