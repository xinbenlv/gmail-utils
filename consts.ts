const CURRENT_DATE_STR = (new Date()).getTime();

export const MAX_PAGE = 1000;
export const MAX_RESULTS = 500;
export const QUOTA_UNITS_PER_MIN = 10000;
export const SQLITE_DB_NAME = `my_emails_inbox.sqlite3`;


const ADDR_FIELDS = ['from', 'to', 'cc', 'bcc', 'delivered_to', 'reply_to'];

export const CREATE_MSG_TABLE_SQL = `CREATE TABLE IF NOT EXISTS emails (
    MSG_ID TEXT PRIMARY KEY,
    MSG_THREAD_ID TEXT,
    LABEL_IDS TEXT,

    RAW_FROM TEXT,
    RAW_TO TEXT,
    RAW_CC TEXT,
    RAW_BCC TEXT,
    RAW_DELIVERED_TO TEXT,
    RAW_REPLY_TO TEXT,

    DATE_TS INTEGER,
    RAW_SUBJECT TEXT,
    SIZE_EST INTEGER,

    'FROM' TEXT,
    FROM_HOST TEXT,
    FROM_2LD TEXT,
    
    'TO' TEXT,
    TO_HOST TEXT,
    TO_2LD TEXT,

    CC TEXT,
    CC_HOST TEXT,
    CC_2LD TEXT,
    
    BCC TEXT,
    BCC_HOST TEXT,
    BCC_2LD TEXT,
    
    DELIVERED_TO TEXT,
    DELIVERED_TO_HOST TEXT,
    DELIVERED_TO_2LD TEXT,

    REPLY_TO TEXT,
    REPLY_TO_HOST TEXT,
    REPLY_TO_2LD TEXT,

    FOREIGN KEY(MSG_THREAD_ID) REFERENCES threads(THREAD_ID)
);`

export const OUT_DIR = `./output/${CURRENT_DATE_STR}/`;
export const DB_FILEPATH = `./output/${SQLITE_DB_NAME}`;
