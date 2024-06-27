-- database: ../output/all_emails2.sqlite3

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
    COUNT(CASE WHEN e.DATE_TS >= (strftime('%s', 'now', '-180 days') * 1000) THEN 1 ELSE NULL END) AS num_emails_last_6months,
    md.EmailFrom,
    DATETIME(md.MaxDateTS / 1000, 'unixepoch') AS last_email_date,
    DATETIME(md.MinDateTS / 1000, 'unixepoch') AS first_email_date,
    printf("%.2f", (md.MaxDateTS - md.MinDateTS) / 1000.0 / 60 / 60 / 24) AS days_span,
    md.LastRawSubject,
    md.FirstRawSubject
FROM emails e
JOIN MaxMinDates md ON e."FROM" = md.EmailFrom
GROUP BY e."FROM"
HAVING num_emails_last_6months > 0
ORDER BY 
    num_senders DESC,
    num_emails_last_6months DESC,
    last_email_date DESC;
