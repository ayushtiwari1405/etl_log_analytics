-- Load cleaned data
logs = LOAD '$INPUT/cleaned'
USING PigStorage(',')
AS (
    host:chararray,
    raw_date:chararray,
    log_hour:chararray,
    http_method:chararray,
    resource_path:chararray,
    protocol_version:chararray,
    status_code:int,
    bytes:int
);

--------------------------------------------------
-- DATE NORMALIZATION (yyyy-mm-dd)
--------------------------------------------------
logs_fmt = FOREACH logs GENERATE
    host,
    CONCAT(
        CONCAT(SUBSTRING(raw_date, 7, 11), '-07-'),
        SUBSTRING(raw_date, 0, 2)
    ) AS log_date,
    log_hour,
    http_method,
    resource_path,
    protocol_version,
    status_code,
    bytes;

--------------------------------------------------
-- Q1: Daily Traffic
-- key = date|status
--------------------------------------------------
g1 = GROUP logs_fmt BY (log_date, status_code);

q1 = FOREACH g1 GENERATE
    FLATTEN(group) AS (log_date, status_code),
    COUNT(logs_fmt) AS request_count,
    SUM(logs_fmt.bytes) AS total_bytes;

q1_formatted = FOREACH q1 GENERATE
    CONCAT(
        CONCAT(log_date, '|'),
        (chararray)status_code
    ) AS key,
    (int)request_count AS value1,
    (int)total_bytes AS value2,
    (int)0 AS value3;

STORE q1_formatted INTO '$OUTPUT/q1' USING PigStorage('\t');

--------------------------------------------------
-- Q2: Top 20 Resources
-- key = resource_path
--------------------------------------------------
g2 = GROUP logs_fmt BY resource_path;

q2_temp = FOREACH g2 {
    unique_hosts = DISTINCT logs_fmt.host;

    GENERATE
        group AS resource_path,
        COUNT(logs_fmt) AS request_count,
        SUM(logs_fmt.bytes) AS total_bytes,
        COUNT(unique_hosts) AS distinct_host_count;
};

q2_sorted = ORDER q2_temp BY request_count DESC;
q2 = LIMIT q2_sorted 20;

q2_formatted = FOREACH q2 GENERATE
    resource_path AS key,
    (int)request_count AS value1,
    (int)total_bytes AS value2,
    (int)distinct_host_count AS value3;

STORE q2_formatted INTO '$OUTPUT/q2' USING PigStorage('\t');

--------------------------------------------------
-- Q3: Hourly Error Analysis
-- key = date|hour
--------------------------------------------------

g = GROUP logs_fmt BY (log_date, log_hour);

q3 = FOREACH g {

    total_count = COUNT(logs_fmt);

    errors = FILTER logs_fmt BY (status_code >= 400 AND status_code < 600);
    error_count = COUNT(errors);

    GENERATE
        group.log_date AS log_date,
        group.log_hour AS log_hour,
        error_count AS error_request_count,
        total_count AS total_request_count,
        (double)error_count / total_count AS error_rate;
};

q3_formatted = FOREACH q3 GENERATE
    CONCAT(
        CONCAT(log_date, '|'),
        log_hour
    ) AS key,
    (int)error_request_count AS value1,
    (int)total_request_count AS value2,
    error_rate AS value3;

STORE q3_formatted INTO '$OUTPUT/q3' USING PigStorage('\t');

