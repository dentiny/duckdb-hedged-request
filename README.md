# DuckDB Hedged Request File System Extension

A DuckDB extension that wraps file systems with **hedged requests** to dramatically reduce tail latency in distributed storage operations.

## Loading hedged request filesystem
Since DuckDB v1.0.0, cache httpfs can be loaded as a community extension without requiring the `unsigned` flag. From any DuckDB instance, the following two commands will allow you to install and load the extension:
```sql
FORCE INSTALL hedged_request_fs from community;
LOAD hedged_request_fs;
```

## Motivation: Cutting Down Tail Latency

When querying data from remote storage systems (S3, HTTP endpoints, etc.), you often experience **tail latency** - those occasional slow requests that significantly impact query performance. A single slow file open or metadata operation can delay your entire query.

This extension addresses tail latency via hedged request, namely a client first sends one request to the replica believed to be the most appropriate, but then falls back on sending a secondary request after some brief delay. The client cancels remaining outstanding requests once the first result is received.  Although naive implementations of this technique typically add unacceptable additional load, many variations exist that give most of the latency-reduction effects while increasing load only modestly.

For more details, please refer to [tail at scale](https://research.google/pubs/the-tail-at-scale/)

This extension implements hedged requests for certain slow I/O operations in DuckDB, including:
- File opens (`OpenFile`)
- Metadata operations (`FileExists`, `DirectoryExists`, `GetFileSize`, `GetLastModifiedTime`, `GetFileType`, `GetVersionTag`)
- Directory operations (`ListFiles`, `Glob`)

Notice, `Read` is not supported for now because it incurs extra memory allocation and copy for every request.

## Usage

### Basic Usage

After loading the extension, wrap your target file system with hedged requests:

```sql
-- List available file systems
SELECT * FROM hedged_fs_list_filesystems();

-- Wrap S3 filesystem with hedged request filesystem
SELECT hedged_fs_wrap('S3FileSystem');

-- Now aforementioned S3 operations will use hedged requests, with no need to change your query
SELECT * FROM 's3://my-bucket/data.parquet';
```

### Configuration

The extension provides several configuration options to tune hedged request behavior:

```sql
-- Configure delay thresholds (in milliseconds) for each operation
SET hedged_fs_open_file_delay_ms = 3000;           -- Default: 3000ms
SET hedged_fs_glob_delay_ms = 5000;                -- Default: 5000ms
SET hedged_fs_file_exists_delay_ms = 3000;         -- Default: 3000ms
SET hedged_fs_directory_exists_delay_ms = 3000;    -- Default: 3000ms
SET hedged_fs_get_file_size_delay_ms = 3000;       -- Default: 3000ms
SET hedged_fs_get_last_modified_time_delay_ms = 3000; -- Default: 3000ms
SET hedged_fs_get_file_type_delay_ms = 3000;        -- Default: 3000ms
SET hedged_fs_get_version_tag_delay_ms = 3000;     -- Default: 3000ms
SET hedged_fs_list_files_delay_ms = 5000;          -- Default: 5000ms

-- Configure maximum number of hedged requests to spawn, which is used to avoid excessive API calls
SET hedged_fs_max_hedged_request_count = 3;        -- Default: 3
```
