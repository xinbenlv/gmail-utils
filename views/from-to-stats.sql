-- database: /Users/zzn/ws/@xinbenlv/gmail-utils/output/my_emails_inbox.sqlite3

-- Use the ▷ button in the top right corner to run the entire file.

SELECT 
  COUNT(MSG_ID) msg_count,
  MAX(DATE_TS),
  MIN(DATE_TS),
  "FROM",
  "FROM_HOST",
  "FROM_2LD",
  "TO",
  "TO_HOST",
  "TO_2LD"
FROM "emails" 
GROUP BY 
  "FROM",
  "FROM_HOST",
  "FROM_2LD",
  "TO",
  "TO_HOST",
  "TO_2LD"
ORDER BY msg_count DESC;
