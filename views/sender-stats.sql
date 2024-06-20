-- database: ../output/my_emails_inbox.sqlite3

-- Use the ▷ button in the top right corner to run the entire file.

-- show fields of talbe emails;

SELECT name FROM pragma_table_info('emails');

WITH MaxDate AS (
    SELECT
        FROM_2LD,
        MAX(DATE_TS) AS MaxDateTS
    FROM emails
    GROUP BY FROM_2LD
),

MinDate AS (
    SELECT
        FROM_2LD,
        MIN(DATE_TS) AS MinDateTS
    FROM emails
    GROUP BY FROM_2LD
)

SELECT
    COUNT(*) as num_emails,
    COUNT(DISTINCT e."FROM") as num_senders,
    e.FROM_2LD,
    DATETIME(MAX(e.DATE_TS)/1000, 'unixepoch') AS last_email_date,
    DATETIME(MIN(e.DATE_TS)/1000, 'unixepoch') AS first_email_date,
    printf("%.2f", CAST((MAX(e.DATE_TS) - MIN(e.DATE_TS)) as REAL) / 1000.0 / 60 / 60 / 24) as days_span,
    printf("%.2f", CAST((MAX(e.DATE_TS) - MIN(e.DATE_TS)) as REAL) / 1000.0 / 60 / 60 / 24 / COUNT(*)) as avg_days_between_emails,
    (SELECT RAW_SUBJECT
     FROM emails
     WHERE FROM_2LD = MaxDate.FROM_2LD AND DATE_TS = MaxDate.MaxDateTS) AS last_raw_subject,
    (SELECT RAW_SUBJECT
     FROM emails
     WHERE FROM_2LD = MinDate.FROM_2LD AND DATE_TS = MinDate.MinDateTS) AS first_raw_subject


FROM emails e
JOIN MaxDate
ON e.FROM_2LD = MaxDate.FROM_2LD
JOIN MinDate
ON e.FROM_2LD = MinDate.FROM_2LD
GROUP BY e.FROM_2LD
ORDER BY num_senders DESC, num_emails DESC;