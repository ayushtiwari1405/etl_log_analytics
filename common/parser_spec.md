# Parser Specification — NASA HTTP Logs

## 1. Input Format

Each line in the dataset represents one HTTP request in Common Log Format:

Example:
199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245

---

## 2. Parsing Strategy

We use the following regular expression to extract core fields:

^(\S+) \S+ \S+ [([^]]+)] "([^"]*)" (\d{3}) (\S+)

### Captured Groups:

1. host → client IP or hostname
2. timestamp → full timestamp string
3. request → raw request string
4. status_code → HTTP status code
5. bytes → response size

---

## 3. Request Field Parsing

The request field is further split by space:

Example:
"GET /history/apollo/ HTTP/1.0"

Parsed as:

* method → GET
* resource_path → /history/apollo/
* protocol → HTTP/1.0

If the request field does not contain at least 3 parts, the record is considered malformed.

---

## 4. Timestamp Processing

Example timestamp:
01/Jul/1995:00:00:01

Derived fields:

* log_date → 1995-07-01 (YYYY-MM-DD)
* log_hour → 00

Month mapping:
Jan→01, Feb→02, ..., Dec→12

---

## 5. Bytes Handling

If bytes field is "-":
→ treat as 0

Else:
→ convert to integer

---

## 6. Malformed Records

A record is considered malformed if:

* regex does not match
* request field cannot be parsed

Malformed records:

* must NOT be dropped silently
* must be counted and reported

---

## 7. Output Fields

Each valid record must produce:

* host (string)
* timestamp (string)
* log_date (string: YYYY-MM-DD)
* log_hour (string: HH)
* method (string)
* resource_path (string)
* protocol (string)
* status_code (integer)
* bytes (integer)

---

## 8. Consistency Requirement

This parsing logic MUST remain identical across all pipelines:

* Apache Pig
* MapReduce
* MongoDB
* Hive

