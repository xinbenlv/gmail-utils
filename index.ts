const fs = require('fs');
const readline = require('readline');
const {google} = require('googleapis');

// If modifying these scopes, delete token.json.
const SCOPES = ['https://www.googleapis.com/auth/gmail.modify'];
// The file token.json stores the user's access and refresh tokens, and is
// created automatically when the authorization flow completes for the first
// time.
const TOKEN_PATH = 'token.json';

// Load client secrets from a local file.
fs.readFile('credentials.json', (err:any, content:string) => {
  if (err) return console.log('Error loading client secret file:', err);
  // Authorize a client with credentials, then call the Gmail API.
  authorize(JSON.parse(content), listMsgSenders);
});

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
  let started = new Date();
  const gmail = google.gmail({version: 'v1', auth});
  let nextPageToken = "";
  let threadList = [];
  let MAX_PAGE = 3;
  let pageNum = 0;
  do {
    let res = await gmail.users.threads.list({
        userId: 'me',
        maxResults: 50,
        pageToken: nextPageToken
    });

    console.log(`Received ${res.data.threads.length} threads, currently ${threadList.length}, nextPageToken = ${res.data.nextPageToken}, time elapsed ${Math.floor(new Date().getTime() - started.getTime())/1000.0} seconds.`);
    nextPageToken = res.data.nextPageToken;
    threadList.push(...res.data.threads);
    pageNum++;
  } while(pageNum < MAX_PAGE && nextPageToken);
  threadList.forEach(async msg => {
    let msgDetails = await gmail.users.threads.get({userId:"me", id:msg.id});
    let recipient = msgDetails.data.messages[0].payload.headers.find(h => h.name === "From").value;
    if (/<.*@.*>/.test(recipient)) {
      recipient = recipient.substring(recipient.indexOf("<") + 1, recipient.indexOf(">"))
    }
    recipient = recipient.toLowerCase();
    console.log(recipient);
  });
  console.log(`Total threads length ${threadList.length}`);
};

module.exports = {
  SCOPES,
  listLabels: listMsgSenders,
};
