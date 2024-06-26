import { DB_FILEPATH, OUT_DIR, CREATE_MSG_TABLE_SQL } from "./consts";
import Database from 'better-sqlite3';
const fs = require('fs');
const addrparser = require('address-rfc2822');
const ADDR_FIELDS = ['from', 'to', 'cc', 'bcc', 'reply_to', 'delivered_to'];
async function main() {
    console.log("start!");
    const db = new Database(DB_FILEPATH, { readonly: false });
    // await db.exec(CREATE_MSG_TABLE_SQL);
    // if (!fs.existsSync(OUT_DIR)) {
    //     fs.mkdirSync(OUT_DIR, { recursive: true });
    // }
    let entries: any[] = await db.prepare('SELECT * FROM emails').all();
    const pageSize = 10000;
    for (let pageIndex = 0; pageIndex * pageSize < entries.length; pageIndex++) {
        let startIndex = pageIndex * pageSize;
        let endIndex = Math.min((pageIndex + 1) * pageSize, entries.length);
        let parsedEntries = entries.slice(startIndex, endIndex)
            .map(entryToParsedFieldRow);
        for (let parsedEntry of parsedEntries) {
            let query = `UPDATE emails SET `;
            ADDR_FIELDS.forEach(field => {
                query += `'${field}' = '${parsedEntry[field]}', `;
                query += `${field}_host = '${parsedEntry[`${field}_host`]}', `;
                query += `${field}_2ld = '${parsedEntry[`${field}_2ld`]}', `;
            });
            // remove the last comma
            query = query.slice(0, -2);
    
            query += ` WHERE msg_id = '${parsedEntry.msgId}'`;
            await db.exec(query);
        }
       console.log(`Updated page (${startIndex}, ${endIndex}) out of total ${entries.length}`);
    }
}
function getAddressHeaders(name) {
    return [
        { id: name, title: name },
        { id: `${name}_host`, title: `${name}_host` },
        { id: `${name}_2ld`, title: `${name}_2ld` },
    ]
}
function parseAddress(addressStr:string) {
    let parsedAddresses;
    try {
        parsedAddresses = addressStr ? addrparser.parse(addressStr) : [];        
        for(let parsed of parsedAddresses) {
            if (!/^[a-zA-Z0-9.!#$%&’*+/=?^_`{|}~-]+@[a-zA-Z0-9-]+(?:\.[a-zA-Z0-9-]+)*$/
            .test(parsed.address)) {
                console.warn("Failed to parse address", addressStr);
                return []; // early
            }
        }
     } catch (err) {
        //console.warn(`Err from parsing from address`, entry);
        parsedAddresses = [];
    }
    return parsedAddresses;

}
// TODO to make it less eror-prone, it's easier to define a schema.
function entryToParsedFieldRow(entry: any) {
    let retOjb = {
        msgId: entry.MSG_ID,
        threadId: entry.MSG_THREAD_ID,
        rawFrom: entry.RAW_FROM,
        date: entry.DATE_TS,
        subject: "",
        sizeEst: entry.SIZE_EST
    };

    ADDR_FIELDS.forEach(field => {
        let addrRaw = entry[`RAW_${field.toUpperCase()}`];
        let addresses = parseAddress(addrRaw);
        retOjb[`${field}`] = addresses.map(a => a.address).join(',').toLowerCase();
        retOjb[`${field}_host`] = addresses.map(a => a.host()).join(',').toLowerCase();
        retOjb[`${field}_2ld`] = addresses.map(a => {
            if (/^(?:[0-9]{1,3}\.){3}[0-9]{1,3}/.test(a.host())) return a.host(); // ip address
            return a.host().split('.')?.slice(-2)?.join('.') || a.host();
        }).join(',').toLowerCase();
    });
    return retOjb;
};

function getHeaders() {
    return [
        { id: 'msgId', title: 'MSG_ID' },
        { id: 'threadId', title: 'THREAD_ID' },
        ...(ADDR_FIELDS.map(t => getAddressHeaders(t))).flat(),
        { id: 'date', title: 'DATE' },
        { id: 'subject', title: 'SUBJECT' },
        { id: 'sizeEst', title: 'SIZE_EST' },
    ];
}

async function writeCsv(entries: any[]) {
    const createCsvWriter = require('csv-writer').createObjectCsvWriter;
    const csvWriter = createCsvWriter({
        path: OUT_DIR + `full_list.csv`,
        header: getHeaders()
    });
    await csvWriter.writeRecords(entries.map(entryToParsedFieldRow));
};


main().then(() => { console.log("done") });
