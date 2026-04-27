-- Load raw logs
raw_logs = LOAD '$INPUT'
USING TextLoader AS (line:chararray);

--------------------------------------------------
-- Parse using SAME regex as MapReduce
--------------------------------------------------
parsed = FOREACH raw_logs GENERATE
    FLATTEN(
        REGEX_EXTRACT_ALL(
            line,
            '^(\\S+) \\S+ \\S+ \\[([^\\]]+)\\] "([^"]*)" (\\d{3}) (\\S+)'
        )
    ) AS (
        host:chararray,
        timestamp:chararray,
        request:chararray,
        status_str:chararray,
        bytes_raw:chararray
    );

--------------------------------------------------
-- Separate malformed (regex failed)
--------------------------------------------------
malformed_1 = FILTER parsed BY host IS NULL OR timestamp IS NULL OR request IS NULL;

valid_1 = FILTER parsed BY host IS NOT NULL AND timestamp IS NOT NULL AND request IS NOT NULL;

--------------------------------------------------
-- Type casting + byte cleaning
--------------------------------------------------
typed = FOREACH valid_1 GENERATE
    host,
    timestamp,
    request,
    (int)status_str AS status,
    (bytes_raw == '-' ? 0 : (int)bytes_raw) AS bytes;

--------------------------------------------------
-- Split request → method, resource, protocol
--------------------------------------------------
req_split = FOREACH typed GENERATE
    host,
    timestamp,
    FLATTEN(STRSPLIT(request, ' ', 3)) AS (method, resource, protocol),
    status,
    bytes;

--------------------------------------------------
-- Handle malformed request splits
--------------------------------------------------
malformed_2 = FILTER req_split BY method IS NULL OR resource IS NULL;

valid_2 = FILTER req_split BY method IS NOT NULL AND resource IS NOT NULL;

--------------------------------------------------
-- Extract date + hour
--------------------------------------------------
final_logs = FOREACH valid_2 GENERATE
    host,
    SUBSTRING(timestamp, 0, 11) AS log_date,
    REGEX_EXTRACT(timestamp, ':(\\d{2}):', 1) AS log_hour,
    method AS http_method,
    resource AS resource_path,
    protocol AS protocol_version,
    status AS status_code,
    bytes;

--------------------------------------------------
-- Store outputs
--------------------------------------------------
STORE final_logs INTO '$OUTPUT/cleaned' USING PigStorage(',');

malformed_all = UNION malformed_1, malformed_2;
STORE malformed_all INTO '$OUTPUT/malformed' USING PigStorage(',');