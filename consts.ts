const CURRENT_DATE_STR = (new Date()).getTime();

export const MAX_PAGE = 1500;
export const MAX_RESULTS = 500;
export const QUOTA_UNITS_PER_MIN = 10000;
export const SQLITE_DB_NAME = `my_emails1.sqlite3`;

export const OUT_DIR = `output/${CURRENT_DATE_STR}/`;
export const DB_FILEPATH = `output/${SQLITE_DB_NAME}`;
