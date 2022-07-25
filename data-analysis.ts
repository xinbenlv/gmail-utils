import { DB_NAME, OUT_DIR } from "./consts";

const { AceBase } = require('acebase');
const db = new AceBase(DB_NAME, {logLevel: 'warn', readOnly: true});

async function main() {
    await db.ready();

    let count = await db.ref('msg').count();
    let msgObjs = (await db.ref('msg').get()).val();
    console.log(msgObjs);
    console.log(`Count = ${count}`);
    let emailList = Object.values(msgObjs).map((msg: any) => msg.from);
    await extractCount(emailList, `Email`);

    let domainList = Object.values(msgObjs).map((msg: any) => msg.from.split('@')[1]);
    await extractCount(domainList, `Domain`);

    let rootDomainList = Object.values(msgObjs).map((msg: any) => msg.from.split('@')[1]).map(domain=> domain.split('.').slice(-2).join('.'));
    await extractCount(rootDomainList, `RootDomain`);
}

async function extractCount(nameList: string[], category:string) {
    let strCounters = {};
    for (let name of nameList) {
        if (!strCounters[name]) {
            strCounters[name] = 0;
        }
        strCounters[name]++;
    }
    let sortedNameArray = sortObject(strCounters);
    console.log(`---------------------${category} List:-------------`);
    for (let i = 0; i < sortedNameArray.length; i++) {
        console.log(`${sortedNameArray[i].key}\t${sortedNameArray[i].value}`);
    }

    const createCsvWriter = require('csv-writer').createObjectCsvWriter;
    const csvWriter = createCsvWriter({
        path: OUT_DIR + `${category}_list.csv`,
        header: [
            {id: 'name', title: 'NAME'},
            {id: 'count', title: 'COUNT'}
        ]
    });
    await csvWriter.writeRecords(sortedNameArray.map(i=>({name: i.key, count: i.value})));

}

function sortObject(obj) {
    var arr = [];
    for (var prop in obj) {
        if (obj.hasOwnProperty(prop)) {
            arr.push({
                'key': prop,
                'value': obj[prop]
            });
        }
    }
    arr.sort(function(a, b) {
        return b.value - a.value;
    });
    return arr;
}

main().then(()=>{console.log("done")});
