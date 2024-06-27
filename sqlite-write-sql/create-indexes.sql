-- database: ../output/all_emails2.sqlite3

CREATE INDEX IF NOT EXISTS idx_from ON emails("FROM");
CREATE INDEX IF NOT EXISTS idx_from_2ld ON emails(FROM_2LD);
CREATE INDEX IF NOT EXISTS idx_email_from ON emails("FROM");
CREATE INDEX IF NOT EXISTS idx_from_2ld_date_ts ON emails(FROM_2LD, DATE_TS);

SELECT name FROM sqlite_master 
WHERE type='index' AND tbl_name='emails' AND sql LIKE '%FROM%';

