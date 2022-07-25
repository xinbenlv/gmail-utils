import readline = require('readline');
import { google } from 'googleapis';
import { RateLimiter } from "limiter";
import {MAX_PAGE, MAX_RESULTS, QUOTA_UNITS_PER_MIN, DB_NAME} from './consts';
const fs = require('fs');


const limiter = new RateLimiter({
  tokensPerInterval: QUOTA_UNITS_PER_MIN,
  interval: "minute",
  fireImmediately: false
});

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
  const { AceBase } = require('acebase');
  const options = { logLevel: 'warn', storage: { path: '.' } }; // optional settings
  const db = new AceBase(DB_NAME, options); // nodejs
  await db.ready();
  let msgObjs = (await db.ref('msg').get()).val();
  let knownMsgIds  = new Set([]);
  if (msgObjs) knownMsgIds = new Set(Object.keys(msgObjs));
  console.log(`Known Msgs number = ${knownMsgIds.size}`);
  let profile = await gmail.users.getProfile({userId: 'me'});
  console.log(`user profile: `,profile);

  do {
    let started = new Date();
    let res = await gApiRateLimit(async ()=> await gmail.users.threads.list({
        userId: 'me',
        labelIds: ['INBOX'],
        maxResults: MAX_RESULTS,
        pageToken: nextPageToken
    }), 10);
    console.log(`Received ${res.data.threads.length} threads, currently at Page ${pageNum}, nextPageToken = ${res.data.nextPageToken}, estimated size ${res.data.resultSizeEstimate}, time elapsed ${Math.floor(new Date().getTime() - started.getTime())/1000.0} seconds.`);
    nextPageToken = res.data.nextPageToken;
    let currentThreadList = res.data.threads;
    let skipped = 0;
    await Promise.all(currentThreadList.map(
      async msg => {
        if (knownMsgIds.has(msg.id)) {
          skipped ++;
          //console.log(`Skip ${msg.id} because it already exists.`);
          return;
        }
        let msgDetails = await gApiRateLimit(async ()=> await gmail.users.threads.get({userId:"me", id:msg.id}), 10);
        let recipient = msgDetails.data.messages[0].payload.headers.find(h => h.name === "From")?.value;
        let toRaw = msgDetails.data.messages[0].payload.headers.find(h => h.name === "To")?.value;
        let subject = msgDetails.data.messages[0].payload.headers.find(h => h.name === "Subject")?.value;
        let internalDate = msgDetails.data.messages[0].internalDate;
        let sizeEstimate = msgDetails.data.messages[0].sizeEstimate;
        if (!recipient) return;
        if (/<.*@.*>/.test(recipient)) {
          recipient = recipient.substring(recipient.indexOf("<") + 1, recipient.indexOf(">"))
        }
        recipient = recipient.toLowerCase();
        // console.log(recipient);
        const updateJson = {};
        updateJson[msg.id] = {
          id: msg.id,
          mailSize: sizeEstimate,
          from: recipient,
          subject: subject,
          internalDate: internalDate,
          toRaw: toRaw || "",
        };
        return db.ref('msg')
        .update(updateJson)
      }
    ));
    console.log(`Done fetching message with page ${pageNum}, skipped ${skipped} threads, time elapsed ${Math.floor(new Date().getTime() - started.getTime())/1000.0} seconds.`);
    pageNum++;
  } while(pageNum < MAX_PAGE && nextPageToken);
};

module.exports = {
  SCOPES,
  listLabels: listMsgSenders,
};

main().then(()=>{console.log("done")});
