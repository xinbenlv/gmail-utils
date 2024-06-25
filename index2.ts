// Patial sync https://developers.google.com/gmail/api/guides/sync
import readline = require('readline');
import { google } from 'googleapis';
import { RateLimiter } from "limiter";
import {MAX_PAGE, MAX_RESULTS, QUOTA_UNITS_PER_MIN, DB_FILEPATH2, CREATE_MSG_TABLE_SQL} from './consts';
import {Database} from 'sqlite3';
import { open } from 'sqlite';
import axios from 'axios';
var Batchelor = require('batchelor');

const dotenv = require('dotenv');
dotenv.config();
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

async function authorizeAsync():Promise<any> {
  return new Promise((resolve, reject) => {
    fs.readFile('credentials.json', async (err:any, content:string) => {
      if (err) {
        console.log('Error loading client secret file:', err);
        reject(err);
      }
      const credentials = JSON.parse(content);
      // Authorize a client with credentials, then call the Gmail API.
      // logout/unauthorize first
      const {client_secret, client_id, redirect_uris} = credentials.installed;
      const oAuth2Client = new google.auth.OAuth2(
          client_id, client_secret, redirect_uris[0]);
  
      // Check if we have previously stored a token.
      fs.readFile(TOKEN_PATH, (err, token) => {
        if (err) return getNewToken(oAuth2Client, resolve);
        oAuth2Client.setCredentials(JSON.parse(token));
        resolve(oAuth2Client);
      });
    });
  });
}


async function fetchMessagesInBatch(auth, messageIds: string[]) {

  const accessToken = await auth.refreshAccessToken();
  console.log("XXX accessToken = ", accessToken);
  const batchEndpoint = 'https://gmail.googleapis.com/batch/gmail/v1'; // Gmail batch endpoint
  let batchRequestBody = '';

  // Construct the batch request body
  messageIds.forEach((id, index) => {
    batchRequestBody += `--batch_boundary\nContent-Type: application/http\n\nGET /gmail/v1/users/me/messages/${id}\n\n`;
  });
  batchRequestBody += `--batch_boundary--`;

  // Make the batch request
  const response = await axios.post(batchEndpoint, batchRequestBody, {
    headers: {
      // Oauth2 access token
      Authorization: `Bearer ${accessToken.token}`,
      'Content-Type': 'multipart/mixed; boundary=batch_boundary'
    }
  });

  // Parse the batch response
  // Note: You'll need to implement the parsing of the multipart response to extract individual messages
  console.log("Batch request response:", response.data);
  // Implement parsing logic here...

  return response.data; // Return the parsed response
}

async function main() {
  // if the directory is not exist, create it
  // get directory name
  let dirName = DB_FILEPATH2.split('/').slice(0, -1).join('/');
  console.log(`dirName = ${dirName}`);
  if (!fs.existsSync(dirName)) {
    fs.mkdirSync(dirName, { recursive: true });
  }

  dbSqlite3 = await open({
    filename: DB_FILEPATH2,
    driver: Database
  });
  await dbSqlite3.exec(CREATE_MSG_TABLE_SQL);
  console.log("Done creating table");
  
  let auth = await authorizeAsync();
  let gmail = google.gmail({version: 'v1', auth});
  let result = await gmail.users.messages.list({
    userId: 'me',
    maxResults: MAX_RESULTS,
  });

  let messageIds = result.data.messages.map(item => item.id).slice(0,3);

  var batch = new Batchelor({
    // Any batch uri endpoint in the form: https://www.googleapis.com/batch/<api>/<version>
    'uri':'https://www.googleapis.com/batch/gmail/v1/',
    'method':'POST',
    'auth': {
      'bearer': [auth.credentials.access_token]
    },
    'headers': {
      'Content-Type': 'multipart/mixed'
    }
  });
  messageIds.forEach(element => {
    batch.add({
      'method':'GET',
      'path':`/gmail/v1/users/me/messages/${element}`
    });
  });

  
  batch.run(function(err, response){
    if (err){
      console.log("XXX Error: " + err.toString());
    } else {
      response.parts.forEach(part => {
        console.log("XXX Response: ", JSON.stringify(part.body, null, 2));
      } );
    }
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

  const extraQuery = {};
  if (process.env.INBOX_ONLY) { 
    extraQuery['labelIds'] = ['INBOX'];
  } else {
    console.log("INBOX_ONLY is not set");
  }
  if (process.env.EXTRA_QUERY) {
    extraQuery['q'] = process.env.EXTRA_QUERY;
  } else {
    console.log("EXTRA_QUERY is not set");
  }
  
  do {
    let started = new Date();
    let baseQuery = {
      userId: 'me',
      maxResults: MAX_RESULTS,
      pageToken: nextPageToken
    };
    let queryObj = Object.assign({}, baseQuery, extraQuery);
    let res = await gApiRateLimit(async ()=> await gmail.users.threads.list(queryObj), 100);
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
              msg.labelIds?.join(',') || "",

              rawFrom,
              rawTo,
              rawCc,
              rawBcc,
              rawDeliveredTo,
              rawReplyTo,

              internalDate,
              rawSubject,
              sizeEstimate,
              // and 18 empty fields
              "", "", "",
              "", "", "",
              "", "", "",
              "", "", "",
              "", "", "",
              "", "", "",
            ];

            // Change to insert or update with conflict
            
            let sqlStr = SqlString.format(`INSERT OR REPLACE INTO emails VALUES (${fields.map(f=>'?').join(',')})`, fields);
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
  // TODO: currently we get known threadIds from Database. If we need to refresh the database, we will need to get known threads Id from memory (and store into memory).
  const result = await dbSqlite3.all('SELECT MSG_THREAD_ID FROM emails');
  return new Set(result.map(obj => obj.MSG_THREAD_ID));
}

module.exports = {
  SCOPES,
  listLabels: listMsgSenders,
};

main().then(()=>{console.log("done")});
