import readline = require('readline');
import { google } from 'googleapis';
import { RateLimiter } from "limiter";
import {MAX_PAGE, MAX_RESULTS, QUOTA_UNITS_PER_MIN, DB_FILEPATH, CREATE_MSG_TABLE_SQL} from './consts';
import {Database} from 'sqlite3';
import { open } from 'sqlite';
const SqlString = require('sqlstring-sqlite');
const fs = require('fs');
const limiter = new RateLimiter({
  tokensPerInterval: QUOTA_UNITS_PER_MIN,
  interval: "minute",
  fireImmediately: false
});
let dbSqlite3;

// If modifying these scopes, delete token.json.
const SCOPES = ['https://www.googleapis.com/auth/gmail.readonly'];
// The file token.json stores the user's access and refresh tokens, and is
// created automatically when the authorization flow completes for the first
// time.
const TOKEN_PATH = 'token.json';

async function gApiRateLimit(callback, quotaUnit) {
  await limiter.removeTokens(quotaUnit);
  return await callback();
}

async function main() {
  // if the directory is not exist, create it
  // get directory name
  let dirName = DB_FILEPATH.split('/').slice(0, -1).join('/');
  console.log(`dirName = ${dirName}`);
  if (!fs.existsSync(dirName)) {
    fs.mkdirSync(dirName, { recursive: true });
  }
  dbSqlite3 = await open({
    filename: DB_FILEPATH,
    driver: Database
  });
  await dbSqlite3.exec(CREATE_MSG_TABLE_SQL);

  // Load client secrets from a local file.
  fs.readFile('credentials.json', (err:any, content:string) => {
    if (err) return console.log('Error loading client secret file:', err);
    // Authorize a client with credentials, then call the Gmail API.
    authorize(JSON.parse(content), listMsgSenders);
  });
}

/**
 * Create an OAuth2 client with the given credentials, and then execute the
 * given callback function.
 * @param {Object} credentials The authorization client credentials.
 * @param {function} callback The callback to call with the authorized client.
 */
function authorize(credentials, callback) {
  const {client_secret, client_id, redirect_uris} = credentials.installed;
  const oAuth2Client = new google.auth.OAuth2(
      client_id, client_secret, redirect_uris[0]);

  // Check if we have previously stored a token.
  fs.readFile(TOKEN_PATH, (err, token) => {
    if (err) return getNewToken(oAuth2Client, callback);
    oAuth2Client.setCredentials(JSON.parse(token));
    callback(oAuth2Client);
  });
}

/**
 * Get and store new token after prompting for user authorization, and then
 * execute the given callback with the authorized OAuth2 client.
 * @param {google.auth.OAuth2} oAuth2Client The OAuth2 client to get token for.
 * @param {getEventsCallback} callback The callback for the authorized client.
 */
function getNewToken(oAuth2Client, callback) {
  const authUrl = oAuth2Client.generateAuthUrl({
    access_type: 'offline',
    scope: SCOPES,
  });
  console.log('Authorize this app by visiting this url:', authUrl);
  const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
  });
  rl.question('Enter the code from that page here: ', (code) => {
    rl.close();
    oAuth2Client.getToken(code, (err, token) => {
      if (err) return console.error('Error retrieving access token', err);
      oAuth2Client.setCredentials(token);
      // Store the token to disk for later program executions
      fs.writeFile(TOKEN_PATH, JSON.stringify(token), (err) => {
        if (err) return console.error(err);
        console.log('Token stored to', TOKEN_PATH);
      });
      callback(oAuth2Client);
    });
  });
}

/**
 * Lists the labels in the user's account.
 *
 * @param {google.auth.OAuth2} auth An authorized OAuth2 client.
 */
async function listMsgSenders(auth) {
  const gmail = google.gmail({version: 'v1', auth});
  let nextPageToken = "";
  let pageNum = 0;
  let knownThreadIds  = await getKnownThreadIds();
  console.log(`Known Msgs number = ${knownThreadIds.size}`);
  let profile = await gmail.users.getProfile({userId: 'me'});
  console.log(`user profile: `,profile);

  do {
    let started = new Date();
    let res = await gApiRateLimit(async ()=> await gmail.users.threads.list({
        userId: 'me',
        labelIds: ['INBOX'],
        maxResults: MAX_RESULTS,
        pageToken: nextPageToken
    }), 100);
    console.log(`${new Date().toISOString()} Received ${res.data.threads.length} threads, currently at Page ${pageNum}, nextPageToken = ${res.data.nextPageToken}, estimated size ${res.data.resultSizeEstimate}, time elapsed ${Math.floor(new Date().getTime() - started.getTime())/1000.0} seconds.`);
    nextPageToken = res.data.nextPageToken;
    let threadList = res.data.threads;

    let skipped = 0;
    await Promise.all(threadList.map(
      async thread => {
        try {
          if (knownThreadIds.has(thread.id)) {
            skipped ++;
            return;
          }
          let threadRes = await gApiRateLimit(async ()=> await gmail.users.threads.get({userId:"me", id:thread.id}), 20);
          Promise.all(threadRes.data.messages.map(async msg=> {
            let rawFrom = msg.payload.headers.find(h => h.name === "From")?.value;
            let rawTo = msg.payload.headers.find(h => h.name === "To")?.value;
            let rawCc = msg.payload.headers.find(h => h.name === "Cc")?.value;
            let rawBcc = msg.payload.headers.find(h => h.name === "Bcc")?.value;
            let rawSubject = msg.payload.headers.find(h => h.name === "Subject")?.value;
            let rawDeliveredTo = msg.payload.headers.find(h => h.name === "Delivered-To")?.value;
            let rawReplyTo = msg.payload.headers.find(h => h.name === "Reply-To")?.value;
            let internalDate = msg.internalDate;
            let sizeEstimate = msg.sizeEstimate;
            let fields = [
              msg.id,
              msg.threadId,
              msg.labelIds.join(','),

              rawFrom,
              rawTo,
              rawCc,
              rawBcc,
              rawDeliveredTo,
              rawReplyTo,

              internalDate,
              rawSubject,
              sizeEstimate
            ];
            let sqlStr = SqlString.format(`INSERT OR IGNORE INTO emails VALUES (${fields.map(f=>'?').join(',')})`, fields);
            await dbSqlite3.exec(sqlStr);
          }));

        } catch (err) {
          console.warn(`Error in getting message meta`, err);
        }
      }
    ));
    console.log(`${new Date().toISOString()} Done fetching message with page ${pageNum}, skipped ${skipped} threads, time elapsed ${Math.floor(new Date().getTime() - started.getTime())/1000.0} seconds.`);
    pageNum++;
  } while(pageNum < MAX_PAGE && nextPageToken);
};

async function getKnownThreadIds():Promise<Set<String>> {
  const result = await dbSqlite3.all('SELECT MSG_THREAD_ID FROM emails');
  return new Set(result.map(obj => obj.MSG_THREAD_ID));
}

module.exports = {
  SCOPES,
  listLabels: listMsgSenders,
};

main().then(()=>{console.log("done")});
