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

async function batchRetrieveMessage(
  auth:any, 
  messageIds:string[], 
  historyId?:string
) {

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

  let res = await new Promise((resolve, reject) => {
    batch.run(function(err, response){
      if (err){
        reject(err);
      } else {
        resolve(response);
      }
    });
  });
  const parseGmailBatchGetResponse = (res) => {
        /* example
      // each part of the parts array is a response to a single request

Response:  {
  "parts": [
    {
      "statusCode": "200",
      "statusMessage": "OK",
      "headers": {
        "Content-Type": "application/json; charset=UTF-8",
        "Vary": "Referer",
        "Content-ID": "Batchelor_a9b0a49035b91e12fc4b5d80c57e320a"
      },
      "body": {
        "id": "1acd3c844e932646",
        "threadId": "18812cea859d6605",
        "snippet": "",
        "payload": {
          "partId": "",
          "mimeType": "multipart/alternative",
          "filename": "",
          "headers": [
            {
              "name": "MIME-Version",
              "value": "1.0"
            },
            {
              "name": "Date",
              "value": "Fri, 12 May 2028 19:52:00 -0700"
            },
            {
              "name": "Message-ID",
              "value": "<CAMVAm0xDyR7guLouB-aZuQ4+rVQm+Cq3+AeyQUhWTSVUbLp3yg@mail.gmail.com>"
            },
            {
              "name": "Subject",
              "value": "mia预测云燕5年内买钢琴或者电钢琴"
            },
            {
              "name": "From",
              "value": "Zainan Victor Zhou <zzn@zzn.im>"
            },
            {
              "name": "To",
              "value": "Mia Jia <JiaMeng1991@gmail.com>"
            },
            {
              "name": "Content-Type",
              "value": "multipart/alternative; boundary=\"000000000000cdd6af05fb8978a2\""
            }
          ],
          "body": {
            "size": 0
          },
          "parts": [
            {
              "partId": "0",
              "mimeType": "text/plain",
              "filename": "",
              "headers": [
                {
                  "name": "Content-Type",
                  "value": "text/plain; charset=\"UTF-8\""
                }
              ],
              "body": {
                "size": 2,
                "data": "DQo="
              }
            },
            {
              "partId": "1",
              "mimeType": "text/html",
              "filename": "",
              "headers": [
                {
                  "name": "Content-Type",
                  "value": "text/html; charset=\"UTF-8\""
                }
              ],
              "body": {
                "size": 2,
                "data": "DQo="
              }
            }
          ]
        },
        "sizeEstimate": 576,
        "historyId": "58000429",
        "internalDate": "1841799120000"
      }
    },
    {
      "statusCode": "200",
      "statusMessage": "OK",
      "headers": {
        "Content-Type": "application/json; charset=UTF-8",
        "Vary": "Referer",
        "Content-ID": "Batchelor_8282686a93fe2be9b92c835cfd2ab099"
      },
      "body": {
        "id": "1973195f28a28354",
        "threadId": "188dfcdebbd62e3d",
        "labelIds": [
          "Label_149",
          "IMPORTANT",
          "Label_145",
          "CATEGORY_PERSONAL"
        ],
        "snippet": "6/01/2025 Remind me to doublecheck HomeDepot Card is paid off Real deadline: 6/13/2025 Context: https://app.clickup.com/t/8684w92nu",
        "payload": {
          "partId": "",
          "mimeType": "multipart/alternative",
          "filename": "",
          "headers": [
            {
              "name": "MIME-Version",
              "value": "1.0"
            },
            {
              "name": "Date",
              "value": "Mon, 2 Jun 2025 10:00:00 -0700"
            },
            {
              "name": "Message-ID",
              "value": "<CAMVAm0wsNAZ2Yq=bx5khBZpmic0AfivNegWai00Q8o260UpaZA@mail.gmail.com>"
            },
            {
              "name": "Subject",
              "value": "6/01/2025 Remind me to doublecheck HomeDepot Card is paid off"
            },
            {
              "name": "From",
              "value": "Zainan Victor Zhou <zzn@zzn.im>"
            },
            {
              "name": "To",
              "value": "\"周载南\" <zzn@zzn.im>, Mia Jia <JiaMeng1991@gmail.com>"
            },
            {
              "name": "Content-Type",
              "value": "multipart/alternative; boundary=\"000000000000faa27a05feaa4775\""
            }
          ],
          "body": {
            "size": 0
          },
          "parts": [
            {
              "partId": "0",
              "mimeType": "text/plain",
              "filename": "",
              "headers": [
                {
                  "name": "Content-Type",
                  "value": "text/plain; charset=\"UTF-8\""
                }
              ],
              "body": {
                "size": 137,
                "data": "Ni8wMS8yMDI1IFJlbWluZCBtZSB0byBkb3VibGVjaGVjayBIb21lRGVwb3QgQ2FyZCBpcyBwYWlkIG9mZg0KDQpSZWFsIGRlYWRsaW5lOiA2LzEzLzIwMjUNCkNvbnRleHQ6IGh0dHBzOi8vYXBwLmNsaWNrdXAuY29tL3QvODY4NHc5Mm51DQo="
              }
            },
            {
              "partId": "1",
              "mimeType": "text/html",
              "filename": "",
              "headers": [
                {
                  "name": "Content-Type",
                  "value": "text/html; charset=\"UTF-8\""
                },
                {
                  "name": "Content-Transfer-Encoding",
                  "value": "quoted-printable"
                }
              ],
              "body": {
                "size": 244,
                "data": "PGRpdiBkaXI9Imx0ciI-Ni8wMS8yMDI1IFJlbWluZCBtZSB0byBkb3VibGVjaGVjayBIb21lRGVwb3QgQ2FyZCBpcyBwYWlkIG9mZjxicj48ZGl2Pjxicj48L2Rpdj48ZGl2PlJlYWwgZGVhZGxpbmU6IDYvMTMvMjAyNTwvZGl2PjxkaXY-Q29udGV4dDrCoDxhIGhyZWY9Imh0dHBzOi8vYXBwLmNsaWNrdXAuY29tL3QvODY4NHc5Mm51Ij5odHRwczovL2FwcC5jbGlja3VwLmNvbS90Lzg2ODR3OTJudTwvYT48L2Rpdj48L2Rpdj4NCg=="
              }
            }
          ]
        },
        "sizeEstimate": 1038,
        "historyId": "65480197",
        "internalDate": "1748883600000"
      }
    },
    {
      "statusCode": "200",
      "statusMessage": "OK",
      "headers": {
        "Content-Type": "application/json; charset=UTF-8",
        "Vary": "Referer",
        "Content-ID": "Batchelor_6fc4c4c8bceb58c7c9aef85b4d7c4443"
      },
      "body": {
        "id": "192cd508857fca93",
        "threadId": "18b70070361c4087",
        "labelIds": [
          "CATEGORY_PERSONAL"
        ],
        "snippet": "伙伴们一年前的这个约定终于到期了，看一眼如何了？ （这是自动定时消息） victor On Fri, Oct 27, 2023 at 09:30 Zainan Victor Zhou &lt;zzn@zzn.im&gt; wrote: 2023.10.27 我们（victor和chase）在汉堡教张媛用metamask，各自打了200美元对应的币，约定一年看哪个受益高 400CRV vs",
        "payload": {
          "partId": "",
          "mimeType": "multipart/alternative",
          "filename": "",
          "headers": [
            {
              "name": "MIME-Version",
              "value": "1.0"
            },
            {
              "name": "Date",
              "value": "Sun, 27 Oct 2024 10:31:00 +0100"
            },
            {
              "name": "References",
              "value": "<CAMVAm0xU3M7kgY5zuv7Q2fRSqmY+mUJfKaMaL_k27oU1Q-9mDw@mail.gmail.com>"
            },
            {
              "name": "In-Reply-To",
              "value": "<CAMVAm0xU3M7kgY5zuv7Q2fRSqmY+mUJfKaMaL_k27oU1Q-9mDw@mail.gmail.com>"
            },
            {
              "name": "Message-ID",
              "value": "<CAMVAm0y8hd4UJLTg6gov9of=+6aoManMpYBH3nA_3fHQ43w9og@mail.gmail.com>"
            },
            {
              "name": "Subject",
              "value": "Re: 一年之约，武当山见"
            },
            {
              "name": "From",
              "value": "Zainan Victor Zhou <zzn@zzn.im>"
            },
            {
              "name": "To",
              "value": "\"chase.wang2022@gmail.com\" <chase.wang2022@gmail.com>, \"张媛\" <zymushaboom@gmail.com>"
            },
            {
              "name": "Content-Type",
              "value": "multipart/alternative; boundary=\"000000000000fcacaf0608adb22c\""
            }
          ],
          "body": {
            "size": 0
          },
          "parts": [
            {
              "partId": "0",
              "mimeType": "text/plain",
              "filename": "",
              "headers": [
                {
                  "name": "Content-Type",
                  "value": "text/plain; charset=\"UTF-8\""
                },
                {
                  "name": "Content-Transfer-Encoding",
                  "value": "base64"
                }
              ],
              "body": {
                "size": 602,
                "data": "5LyZ5Ly05Lus5LiA5bm05YmN55qE6L-Z5Liq57qm5a6a57uI5LqO5Yiw5pyf5LqG77yM55yL5LiA55y85aaC5L2V5LqG77yfDQrvvIjov5nmmK_oh6rliqjlrprml7bmtojmga_vvIkNCg0KdmljdG9yDQoNCk9uIEZyaSwgT2N0IDI3LCAyMDIzIGF0IDA5OjMwIFphaW5hbiBWaWN0b3IgWmhvdSA8enpuQHp6bi5pbT4gd3JvdGU6DQoNCj4gMjAyMy4xMC4yNyDmiJHku6zvvIh2aWN0b3LlkoxjaGFzZe-8ieWcqOaxieWgoeaVmeW8oOWqm-eUqG1ldGFtYXNr77yM5ZCE6Ieq5omT5LqGMjAw576O5YWD5a-55bqU55qE5biB77yM57qm5a6a5LiA5bm055yL5ZOq5Liq5Y-X55uK6auYDQo-DQo-DQo-IDQwMENSViB2cyAwLjExMTZFVEgNCj4NCj4NCj4NCj4gaHR0cHM6Ly9ldGhlcnNjYW4uaW8vdHgvMHg2ODk4ZTA2OGMyYmIwNjJlOTE3MmYyODczMzM1MzdlODk1YTIzMTA2Nzc0ZDU2YjI5OWQ1OTVhMDdjZGM4YmE2P2xvY2FsZT1lbi1VUyZ1dG1fc291cmNlPWltdG9rZW4NCj4NCj4NCj4gaHR0cHM6Ly9ldGhlcnNjYW4uaW8vdHgvMHhlYWEwMGExM2ZhYTdlMTExNzc1NTUxZTYwZTgwYjU2ZGQ0NjMzNDQyYmUxY2M0MDBjMWMwYTFkZTA2OWUyNzAxDQo-DQo-DQo-DQo="
              }
            },
            {
              "partId": "1",
              "mimeType": "text/html",
              "filename": "",
              "headers": [
                {
                  "name": "Content-Type",
                  "value": "text/html; charset=\"UTF-8\""
                },
                {
                  "name": "Content-Transfer-Encoding",
                  "value": "quoted-printable"
                }
              ],
              "body": {
                "size": 1490,
                "data": "PGRpdj48ZGl2IGRpcj0iYXV0byI-5LyZ5Ly05Lus5LiA5bm05YmN55qE6L-Z5Liq57qm5a6a57uI5LqO5Yiw5pyf5LqG77yM55yL5LiA55y85aaC5L2V5LqG77yfPC9kaXY-PGRpdiBkaXI9ImF1dG8iPu-8iOi_meaYr-iHquWKqOWumuaXtua2iOaBr--8iTwvZGl2PjxkaXYgZGlyPSJhdXRvIj48YnI-PC9kaXY-PGRpdiBkaXI9ImF1dG8iPnZpY3RvcjwvZGl2PjxkaXY-PGJyPjxkaXYgY2xhc3M9ImdtYWlsX3F1b3RlIj48ZGl2IGRpcj0ibHRyIiBjbGFzcz0iZ21haWxfYXR0ciI-T24gRnJpLCBPY3QgMjcsIDIwMjMgYXQgMDk6MzAgWmFpbmFuIFZpY3RvciBaaG91ICZsdDs8YSBocmVmPSJtYWlsdG86enpuQHp6bi5pbSIgdGFyZ2V0PSJfYmxhbmsiPnp6bkB6em4uaW08L2E-Jmd0OyB3cm90ZTo8YnI-PC9kaXY-PGJsb2NrcXVvdGUgY2xhc3M9ImdtYWlsX3F1b3RlIiBzdHlsZT0ibWFyZ2luOjBweCAwcHggMHB4IDAuOGV4O2JvcmRlci1sZWZ0LXdpZHRoOjFweDtib3JkZXItbGVmdC1zdHlsZTpzb2xpZDtwYWRkaW5nLWxlZnQ6MWV4O2JvcmRlci1sZWZ0LWNvbG9yOnJnYigyMDQsMjA0LDIwNCkiPjxkaXYgZGlyPSJhdXRvIj4yMDIzLjEwLjI3IOaIkeS7rO-8iHZpY3RvcuWSjGNoYXNl77yJ5Zyo5rGJ5aCh5pWZ5byg5aqb55SobWV0YW1hc2vvvIzlkIToh6rmiZPkuoYyMDDnvo7lhYPlr7nlupTnmoTluIHvvIznuqblrprkuIDlubTnnIvlk6rkuKrlj5fnm4rpq5g8L2Rpdj48ZGl2IGRpcj0iYXV0byI-PGJyPjwvZGl2PjxkaXYgZGlyPSJhdXRvIj48YnI-PC9kaXY-PGRpdj40MDBDUlYgdnMgMC4xMTE2RVRIPGRpdiBkaXI9ImF1dG8iPjxicj48L2Rpdj48ZGl2IGRpcj0iYXV0byI-PGJyPjwvZGl2PjxkaXYgZGlyPSJhdXRvIj48ZGl2PjxhIGhyZWY9Imh0dHBzOi8vZXRoZXJzY2FuLmlvL3R4LzB4Njg5OGUwNjhjMmJiMDYyZTkxNzJmMjg3MzMzNTM3ZTg5NWEyMzEwNjc3NGQ1NmIyOTlkNTk1YTA3Y2RjOGJhNj9sb2NhbGU9ZW4tVVMmYW1wO3V0bV9zb3VyY2U9aW10b2tlbiIgdGFyZ2V0PSJfYmxhbmsiPmh0dHBzOi8vZXRoZXJzY2FuLmlvL3R4LzB4Njg5OGUwNjhjMmJiMDYyZTkxNzJmMjg3MzMzNTM3ZTg5NWEyMzEwNjc3NGQ1NmIyOTlkNTk1YTA3Y2RjOGJhNj9sb2NhbGU9ZW4tVVMmYW1wO3V0bV9zb3VyY2U9aW10b2tlbjwvYT48L2Rpdj48YnI-PC9kaXY-PGRpdiBkaXI9ImF1dG8iPjxkaXY-PGEgaHJlZj0iaHR0cHM6Ly9ldGhlcnNjYW4uaW8vdHgvMHhlYWEwMGExM2ZhYTdlMTExNzc1NTUxZTYwZTgwYjU2ZGQ0NjMzNDQyYmUxY2M0MDBjMWMwYTFkZTA2OWUyNzAxIiB0YXJnZXQ9Il9ibGFuayI-aHR0cHM6Ly9ldGhlcnNjYW4uaW8vdHgvMHhlYWEwMGExM2ZhYTdlMTExNzc1NTUxZTYwZTgwYjU2ZGQ0NjMzNDQyYmUxY2M0MDBjMWMwYTFkZTA2OWUyNzAxPC9hPjwvZGl2Pjxicj48L2Rpdj48ZGl2IGRpcj0iYXV0byI-PGJyPjwvZGl2PjwvZGl2Pg0KPC9ibG9ja3F1b3RlPjwvZGl2PjwvZGl2Pg0KPC9kaXY-DQo="
              }
            }
          ]
        },
        "sizeEstimate": 3716,
        "historyId": "66115530",
        "internalDate": "1730021460000"
      }
    }
  ],
  "errors": 0
}
    */

    return res.parts.map(part => {
      let msg = part.body;
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
      return fields;
      /* parsed as 
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
      */

    });

  };

  let parsedEmails = parseGmailBatchGetResponse(res);
  return parsedEmails;
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

  let messageIds = result.data.messages.map(item => item.id).slice(0,100);
  let parsedEmails = await batchRetrieveMessage(auth, messageIds);
  let insertSql = `INSERT INTO emails VALUES ${parsedEmails.map(email => `(${SqlString.escape(email)})`).join(',')}`;
  console.log("insertSql = ", insertSql);
  await dbSqlite3.exec(insertSql);
  console.log("Done inserting emails");
  await dbSqlite3.close();
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

module.exports = {
  SCOPES,
};

main().then(()=>{console.log("done")});
