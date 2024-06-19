-- database: ../output/my_emails_inbox.sqlite3

-- Use the ▷ button in the top right corner to run the entire file.

SELECT COUNT(*),
    -- DATE_TS from epoch time in ms to human readable time
    DATE(DATE_TS/1000, 'unixepoch') AS date
FROM emails
GROUP BY date
ORDER BY date DESC;