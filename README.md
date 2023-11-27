Gmail utilities.

## Developer

Before running, please follow [this link](https://developers.google.com/workspace/guides/create-credentials#desktop-app) to download the JSON credentials and renamed to `credentials.json` in the repo root dir before using.

The download file link will look like somehting like
https://console.cloud.google.com/apis/credentials?project=<project_id>

Remember to update the project_id with yours.
![Alt text](screenshot.png)


To run

```sh

git clone https://github.com/xinbenlv/gmail-utils.git
cd gmail-utils
npm i
npm start # runs the query, wait for a while, 10min~
npm run da
```
