## Connection Settings (root configuration)

Configure connection to your Hyperion EPM server. These settings are shared across all write rows.

- **Hyperion Server URL** — Base URL including port, e.g. `http://hyperiondev:9000`
- **Username / Password** — Shared Services (CSS) credentials
- **Essbase Host** — Optional override for Essbase hostname (defaults to the host from Hyperion Server URL)
- **Essbase REST API Port** — Port for Essbase 21C REST API (default: 9010)
- **Verify SSL Certificate** — Disable for self-signed certificates in development environments

## Write Row Configuration

Each row defines one write operation from a Keboola Storage table into Hyperion.

- **Module** — `essbase` or `planning`
- **Application** — Essbase or Planning application name
- **Database / Cube** — Essbase cube name (required for Essbase module)
- **Plan Type** — Planning plan type name (required for Planning module)
- **Input Table Name** — Keboola Storage table to read from (without `.csv` extension). Column headers must match Hyperion dimension names.
- **Batch Size** — Number of rows sent per API call (default: 1000)
