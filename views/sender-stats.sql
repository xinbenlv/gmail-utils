-- database: ../output/my_emails_inbox.sqlite3

-- Use the ▷ button in the top right corner to run the entire file.

SELECT
    "FROM"
    , COUNT(*) as num_emails
    , MAX(DATE_TS) as last_email
    -- human readable time
    , DATETIME(MAX(DATE_TS)/1000, 'unixepoch') AS last_email_date
    ,MIN(DATE_TS) as first_email
    -- -- human readable time
    ,DATETIME(MIN(DATE_TS)/1000, 'unixepoch') AS first_email_date
    ,(MAX(DATE_TS) - MIN(DATE_TS)) as time_span
    -- -- human readable timespan in total days
    ,CAST((MAX(DATE_TS) - MIN(DATE_TS)) as REAL) / 1000.0 / 60 / 60 / 24 as days_span

    ,(MAX(DATE_TS) - MIN(DATE_TS)) / COUNT(*) as avg_time_between_emails
    ,CAST((MAX(DATE_TS) - MIN(DATE_TS)) as REAL) / 1000.0 / 60 / 60 / 24 / COUNT(*) as avg_days_between_emails

FROM emails
GROUP BY "FROM"
ORDER BY 1 DESC;
