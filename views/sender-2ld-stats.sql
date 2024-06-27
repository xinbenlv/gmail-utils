-- database: ../output/all_emails2.sqlite3

-- CREATE INDEX idx_from_2ld ON emails(FROM_2LD);

-- CREATE INDEX idx_date_ts ON emails(DATE_TS);

-- CREATE INDEX idx_from_2ld_date_ts ON emails(FROM_2LD, DATE_TS);

-- sqlite3 -header -csv output/all_emails2.sqlite3 ".read views/optimized-sender-stats2.sql" > output/sender_stats.csv

CREATE TEMPORARY TABLE MaxMinDates (
    FROM_2LD TEXT PRIMARY KEY,
    MaxDateTS INTEGER,
    MinDateTS INTEGER,
    LastRawSubject TEXT,
    FirstRawSubject TEXT
);

INSERT INTO MaxMinDates (FROM_2LD, MaxDateTS, MinDateTS, LastRawSubject, FirstRawSubject)
SELECT
    e.FROM_2LD,
    MaxDateTS,
    MinDateTS,
    (SELECT RAW_SUBJECT FROM emails sub WHERE sub.FROM_2LD = e.FROM_2LD AND sub.DATE_TS = MaxDateTS) AS LastRawSubject,
    (SELECT RAW_SUBJECT FROM emails sub WHERE sub.FROM_2LD = e.FROM_2LD AND sub.DATE_TS = MinDateTS) AS FirstRawSubject
FROM (
    SELECT 
        FROM_2LD, 
        MAX(DATE_TS) AS MaxDateTS, 
        MIN(DATE_TS) AS MinDateTS
    FROM emails 
    GROUP BY FROM_2LD
) e;

SELECT
    COUNT(*) AS num_emails,
    COUNT(DISTINCT e."FROM") AS num_senders,
    md.FROM_2LD,
    DATETIME(md.MaxDateTS / 1000, 'unixepoch') AS last_email_date,
    DATETIME(md.MinDateTS / 1000, 'unixepoch') AS first_email_date,
    printf("%.2f", (md.MaxDateTS - md.MinDateTS) / 1000.0 / 60 / 60 / 24) AS days_span,
    md.LastRawSubject,
    md.FirstRawSubject
FROM emails e
JOIN MaxMinDates md ON e.FROM_2LD = md.FROM_2LD
GROUP BY e.FROM_2LD
ORDER BY 
    num_senders DESC,
    num_emails DESC,
    last_email_date DESC;