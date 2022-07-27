import { DB_FILEPATH, OUT_DIR } from "./consts";
import Database from 'better-sqlite3';

const fs = require('fs');
const addrparser = require('address-rfc2822');
async function main() {
    const db = new Database(DB_FILEPATH, { readonly: true });
    await db.exec(`CREATE TABLE IF NOT EXISTS emails (
        MSG_ID TEXT,
        RAW_FROM TEXT,
        RAW_TO TEXT,
        DATE_TS INTEGER
    );`);
    if (!fs.existsSync(OUT_DIR)) {
        fs.mkdirSync(OUT_DIR, { recursive: true });
    }
    let msgObjs: any[] = await db.prepare('SELECT * FROM emails').all();
    console.log(`Count = ${msgObjs.length}`);
    await writeCsv(msgObjs);
}
async function writeCsv(msgOjbs: any[]) {
    const createCsvWriter = require('csv-writer').createObjectCsvWriter;
    const csvWriter = createCsvWriter({
        path: OUT_DIR + `full_list.csv`,
        header: [
            { id: 'msgId', title: 'MSG_ID' },
            { id: 'rawFrom', title: 'RAW_FROM' },
            { id: 'rawTo', title: 'RAW_TO' },
            { id: 'from', title: 'FROM' },
            { id: 'fromHost', title: 'FROM_HOST' },
            { id: 'from2LD', title: 'FROM_2LD' },
            { id: 'to', title: 'to' },
            { id: 'toHost', title: 'TO_HOST' },
            { id: 'to2LD', title: 'TO_2LD' },
            { id: 'date', title: 'DATE' },
            { id: 'subject', title: 'SUBJECT' },
            { id: 'sizeEst', title: 'SIZE_EST' },
        ]
    });
    await csvWriter.writeRecords(msgOjbs.map(entry => {
        let fromAddresses;
        try {
            fromAddresses = entry.RAW_FROM ? addrparser.parse(entry.RAW_FROM) : [];
            fromAddresses[0].host();
        } catch (err) {
            console.warn(`Err from parsing from address`, entry);
            fromAddresses = [];
        }
        let toAddresses;
        try {
            toAddresses = entry.RAW_TO ? addrparser.parse(entry.RAW_TO) : [];
            toAddresses[0].host();
        } catch (err) {
            console.warn(`Err from parsing to address`, entry);
            toAddresses = [];
        }
        return {
            msgId: entry.MSG_ID,
            rawFrom: entry.RAW_FROM,
            from: fromAddresses.map(a => a.address).join(','),
            fromHost: fromAddresses.map(a => a.host()).join(','),
            from2LD: fromAddresses.map(a => a.host().split('.')?.slice(-2)?.join('.') || a.host()).join(','),
            rawTo: entry.RAW_TO,
            to: toAddresses.map(a => a.address).join(','),
            toHost: toAddresses.map(a => a.host()).join(','),
            to2LD: toAddresses.map(a => a.host().split('.')?.slice(-2)?.join('.') || a.host()).join(','),
            date: entry.DATE_TS,
            subject: "",// entry.RAW_SUBJECT,
            sizeEst: entry.SIZE_EST
        };
    }));
};

main().then(() => { console.log("done") });
