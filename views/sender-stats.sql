-- database: ../output/all_emails2.sqlite3

-- CREATE INDEX idx_from_2ld ON emails(FROM_2LD);

-- CREATE INDEX idx_date_ts ON emails(DATE_TS);

-- CREATE INDEX idx_from_2ld_date_ts ON emails(FROM_2LD, DATE_TS);

-- sqlite3 -header -csv output/all_emails2.sqlite3 ".read views/optimized-sender-stats2.sql" > output/sender_stats.csv
-- Drop the existing temporary table if it exists, for safety
DROP TABLE IF EXISTS MaxMinDates;

-- Create a new temporary table focusing on the "FROM" field
CREATE TEMPORARY TABLE MaxMinDates (
    EmailFrom TEXT PRIMARY KEY,
    MaxDateTS INTEGER,
    MinDateTS INTEGER,
    LastRawSubject TEXT,
    FirstRawSubject TEXT
);

-- Populate the temporary table using the "FROM" field for grouping
INSERT INTO MaxMinDates (EmailFrom, MaxDateTS, MinDateTS, LastRawSubject, FirstRawSubject)
SELECT
    e."FROM",
    MaxDateTS,
    MinDateTS,
    (SELECT RAW_SUBJECT FROM emails sub WHERE sub."FROM" = e."FROM" AND sub.DATE_TS = MaxDateTS) AS LastRawSubject,
    (SELECT RAW_SUBJECT FROM emails sub WHERE sub."FROM" = e."FROM" AND sub.DATE_TS = MinDateTS) AS FirstRawSubject
FROM (
    SELECT 
        "FROM", 
        MAX(DATE_TS) AS MaxDateTS, 
        MIN(DATE_TS) AS MinDateTS
    FROM emails 
    GROUP BY "FROM"
) e;

-- Use the temporary table in your main query to compute the desired statistics
SELECT
    COUNT(*) AS num_emails,
    COUNT(DISTINCT e."FROM") AS num_senders,
    md.EmailFrom,
    DATETIME(md.MaxDateTS / 1000, 'unixepoch') AS last_email_date,
    DATETIME(md.MinDateTS / 1000, 'unixepoch') AS first_email_date,
    printf("%.2f", (md.MaxDateTS - md.MinDateTS) / 1000.0 / 60 / 60 / 24) AS days_span,
    md.LastRawSubject,
    md.FirstRawSubject
FROM emails e
JOIN MaxMinDates md ON e."FROM" = md.EmailFrom
GROUP BY e."FROM"
ORDER BY 
    num_senders DESC,
    num_emails DESC,
    last_email_date DESC;