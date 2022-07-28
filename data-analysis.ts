import { DB_FILEPATH, OUT_DIR, CREATE_MSG_TABLE_SQL } from "./consts";
import Database from 'better-sqlite3';

const fs = require('fs');
const addrparser = require('address-rfc2822');
const ADDR_FIELDS = ['from', 'to', 'cc', 'bcc', 'reply_to', 'delivered_to'];
async function main() {
    const db = new Database(DB_FILEPATH, { readonly: true });
    await db.exec(CREATE_MSG_TABLE_SQL);
    if (!fs.existsSync(OUT_DIR)) {
        fs.mkdirSync(OUT_DIR, { recursive: true });
    }
    let msgObjs: any[] = await db.prepare('SELECT * FROM emails').all();
    console.log(`Count = ${msgObjs.length}`);
    await writeCsv(msgObjs);
}
function getAddressHeaders(name) {
    return [
        { id: `${name}_raw`, title: `${name}_raw` },
        { id: name, title: name },
        { id: `${name}_host`, title: `${name}_host` },
        { id: `${name}_2ld`, title: `${name}_2ld` },
    ]
}
function parseAddress(addressStr:string) {
    let parsedAddresses;
    try {
        parsedAddresses = addressStr ? addrparser.parse(addressStr) : [];
        parsedAddresses[0].host();
    } catch (err) {
        //console.warn(`Err from parsing from address`, entry);
        parsedAddresses = [];
    }
    return parsedAddresses;

}
async function writeCsv(msgOjbs: any[]) {
    const createCsvWriter = require('csv-writer').createObjectCsvWriter;
    const csvWriter = createCsvWriter({
        path: OUT_DIR + `full_list.csv`,
        header: [
            { id: 'msgId', title: 'MSG_ID' },
            { id: 'threadId', title: 'THREAD_ID' },
            ...(ADDR_FIELDS.map(t=>getAddressHeaders(t))).flat(),
            { id: 'date', title: 'DATE' },
            { id: 'subject', title: 'SUBJECT' },
            { id: 'sizeEst', title: 'SIZE_EST' },
        ]
    });
    await csvWriter.writeRecords(msgOjbs.slice(2).map(entry => {
        let retOjb = {
            msgId: entry.MSG_ID,
            threadId: entry.MSG_THREAD_ID,
            rawFrom: entry.RAW_FROM,
            date: entry.DATE_TS,
            subject: "",// entry.RAW_SUBJECT,
            sizeEst: entry.SIZE_EST
        };

        ADDR_FIELDS.forEach(field=>{
            let addrRaw = entry[`RAW_${field.toUpperCase()}`];
            let addresses = parseAddress(addrRaw);
            retOjb[`${field}`] = addresses.map(a => a.address).join(',');
            retOjb[`${field}_host`] = addresses.map(a => a.host()).join(',');
            retOjb[`${field}_2ld`] = addresses.map(a => {
                if (/^(?:[0-9]{1,3}\.){3}[0-9]{1,3}/.test(a.host())) return a.host(); // ip address
                return a.host().split('.')?.slice(-2)?.join('.') || a.host();
            }).join(',');
            retOjb[`${field}_raw`] = addrRaw;
        });
        return retOjb;
    }));
};

main().then(() => { console.log("done") });
