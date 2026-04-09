wr-oracle-hyperion
==================

Keboola writer for Oracle Hyperion EPM (Essbase). Loads data from Keboola Storage tables into Essbase BSO databases via the Essbase 21C REST API.

**Table of Contents:**

[TOC]

Functionality Notes
===================

Data is pushed to Essbase using the free-form BSO dataload stream API (3-step: start stream → push text data → commit). Each configuration row represents one write operation to a specific Essbase application/database combination. Multiple rows sharing the same connection are executed sequentially within a single run.

Row members with spaces are automatically double-quoted to comply with the BSO free-form format. Context members (dimensions not represented as CSV columns) are written as header lines before the column row.

Prerequisites
=============

- Essbase 21C (REST API on port 9010 by default)
- Network access from Keboola to the Essbase host
- Essbase user with write permissions to the target application/database
- Input mapping table(s) in Keboola Storage whose columns correspond to Essbase members/dimensions

Features
========

| **Feature**             | **Description**                                                                 |
|-------------------------|---------------------------------------------------------------------------------|
| Generic UI Form         | Dynamic UI form with application/database dropdowns loaded via sync actions.    |
| Row-Based Configuration | Each row targets one Essbase application/database; all rows share one connection.|
| Column Mapping          | Rename and filter CSV columns to Essbase member names before loading.           |
| Context Members         | Specify default members for dimensions not present as CSV columns.              |
| Batched Loading         | Data is sent in configurable batches (default 1000 rows) to avoid timeouts.     |
| SSL Support             | HTTPS with optional certificate verification disable for self-signed certs.     |

Configuration
=============

Connection (root config)
------------------------

| Parameter        | Description                                                                 |
|------------------|-----------------------------------------------------------------------------|
| `essbase_host`   | Hostname or IP of the Essbase server. Mutually exclusive with `hyperion_url`.|
| `hyperion_url`   | Full Hyperion EPM URL — host is extracted from it if `essbase_host` is not set.|
| `essbase_port`   | Essbase REST API port (default: `9010`).                                    |
| `username`       | Essbase username.                                                           |
| `#password`      | Essbase password (encrypted).                                               |
| `use_ssl`        | Use HTTPS (default: `true`).                                                |
| `ssl_verify`     | Verify SSL certificate (default: `true`). Set to `false` for self-signed.  |
| `debug`          | Enable debug logging (default: `false`).                                   |

Write Row (per-row config)
--------------------------

| Parameter               | Description                                                                          |
|-------------------------|--------------------------------------------------------------------------------------|
| `destination.application` | Essbase application name.                                                          |
| `destination.database`    | Essbase database (cube) name within the application.                               |
| `source.input_table`      | Stem of the input mapping table filename. Leave empty to use the first mapped table.|
| `source.column_mapping`   | List of `{source, destination}` objects to rename/filter columns. Empty = pass all. |
| `source.context_members`  | List of member strings for dimensions not present in the CSV columns.               |
| `source.batch_size`       | Rows per API call (default: `1000`).                                                |

Output
======

This is a writer component — it produces no output tables. Successful completion is logged with the total number of rows written per application/database.

Development
-----------

To customize the local data folder path, replace the `CUSTOM_FOLDER` placeholder with your desired path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, initialize the workspace, and run the component using the following commands:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone wr-oracle-hyperion
cd wr-oracle-hyperion
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and perform lint checks using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For details about deployment and integration with Keboola, refer to the
[deployment section of the developer documentation](https://developers.keboola.com/extend/component/deployment/).
