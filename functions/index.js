const functions = require('firebase-functions');
const {region,runtimeOpts} = require( "./util/bigqueryConfig.js")
const {writeFile,parseJsonDataToNewLineDelimited} = require("./util/fileUtil.js")
const {getCollectionList,getScoreFromLog,db} = require( "./util/firebaseUtil.js")
const { BigQuery } = require('@google-cloud/bigquery')

const projectId = 'vondercenter'
const bigquery = new BigQuery({projectId:projectId})


exports.loadFileIntoBigQuery = functions.region(region).runWith(runtimeOpts).https.onRequest(async (req, res) => {
    let dataset = req.query.orgName
    let table = "score"
    let filename = `${dataset}-score.json`
    
    await loadLocalJsonFileToBigQuery(dataset, table, filename)
    res.send(`eazy complete with ${dataset}.${table} use ${filename}`)
})

exports.listSurvey = functions.https.onRequest(async (req, res) => {
    await db.listCollections().then(collections => {
        collections.forEach(collection => {
            console.log('found collection: ', collection.id)
        })
    })
})

exports.getCollectionToJson = functions.region(region).runWith(runtimeOpts).https.onRequest(async (req, res) => {
    let collections = await getCollectionList(req.query.orgName)
    let rawData = []
    let raw_data2 = []
    let toJson = []
    console.log(collections[0])
    for (let i = 0; i < collections.length; i++) {
        console.log("orgName : " + req.query.orgName)
        console.log("collection Name: " + collections[i])
        rawData.push(await getScoreFromLog(req.query.orgName, String(collections[i])))
    }
    console.log(rawData.length)
    rawData = rawData.flat();


    const promisesIndex = rawData.map(async (row) => {

        let lastIndex = await find_playIndex(row)
        let playIndex = lastIndex['last_playIndex'] + 1
        let roundIndex = lastIndex['last_roundIndex'] + 1
        row['playIndex'] = playIndex
        row['roundIndex'] = roundIndex
        raw_data2.push(row)
    })

    await Promise.all(promisesIndex)

    let repeatPlay = []
    const promises = raw_data2.map(async (row, index) => {

        let foundIndex = repeatPlay.findIndex(each => each.employeeId === row.employeeId && each.challengeCode === row.challengeCode)
        if (foundIndex !== -1) {
            repeatPlay[foundIndex]['playIndex'] = repeatPlay[foundIndex].playIndex + 1
            repeatPlay[foundIndex]['roundIndex'] = repeatPlay[foundIndex].roundIndex + 1
            row['playIndex'] = repeatPlay[foundIndex].playIndex
            row['roundIndex'] = repeatPlay[foundIndex].roundIndex
        } else {
            repeatPlay.push(row)
        }

        row.timestamp = String(row.timestamp)
        let answers = row.answers

        await answers.map((answer) => {
            let choiceCode = ""
            if (!answer.choiceCode) {
                console.log('answer: ', answer);
                choiceCode = answer.code
            } else {
                choiceCode = answer.choiceCode.substring(0, 1)
            }
            let answeredAt = answer.timestamp.substring(0, answer.timestamp.length - 2)


            let temp = {
                playId: row.id,
                playIndex: row.playIndex,
                roundIndex: row.roundIndex,
                challengeCode: row.challengeCode,
                employeeId: row.employeeId,
                organizeId: row.orgName,
                questionCode: answer.questionCode,
                choiceCode: choiceCode,
                score: answer.score,
                answeredAt: answeredAt,
                isCorrect: answer.isCorrect
            }
            toJson.push(temp)
            return null
        })
    })

    await Promise.all(promises)
    let result = JSON.stringify(toJson)

    await writeFile(`${req.query.orgName}-score.json`, result)
    console.log("Json size: ", toJson.length)
    res.send(JSON.stringify(toJson)).end()
})

const find_playIndex = async (params) => {
    let { orgName, employeeId, challengeCode } = params

    let promise = new Promise(resolve => {
        let dataSetId = orgName
        let fullTableName = projectId + '.' + dataSetId + '.loaded_data'
        let sql = `SELECT max(playIndex) as last_playIndex, max(roundIndex) as last_roundIndex FROM ${fullTableName} WHERE employeeId='${employeeId}' and challengeCode='${challengeCode}' ;`;
        console.log(sql)

        bigquery.query({
            query: sql,
            useLegacySql: false
        }).then((results) => {
            resolve(results[0])
            return null
        }).catch((error) => {
            resolve(null)
            return null
        })
    })
    let result = await promise
    let { last_playIndex, last_roundIndex } = result ? result[0] : { last_playIndex: 0, last_roundIndex: 0 }
    last_playIndex = (last_playIndex !== null) ? last_playIndex : 0
    last_roundIndex = (last_roundIndex !== null) ? last_roundIndex : 0
    return { 'last_playIndex': last_playIndex, 'last_roundIndex': last_roundIndex }
}

const loadLocalJsonFileToBigQuery = async (datasetName, tableName, filename) => {

    const metadata = {
        sourceFormat: 'NEWLINE_DELIMITED_JSON',
        autodetect: true,
        location: 'ASIA',
        writeDisposition: 'WRITE_APPEND',
        schema:'playId:string, playIndex:integer, roundIndex:integer, challengeCode:string, employeeId:string, organizeId:string, questionCode:string, choiceCode:string, score:integer, answeredAt:datetime, isCorrect:boolean'
    };
    dirname = 'NDJSON-SCORE'


    const [job] = await bigquery
        .dataset(datasetName)
        .table(tableName)
        .load(`${dirname}/${filename}`, metadata);

    console.log(`Job ${job.id} completed.`);


    const errors = job.status.errors;
    if (errors && errors.length > 0) {
        throw errors;
    }
}

exports.parseFile = functions.runWith(runtimeOpts).https.onRequest(async (req, res) => {
    let orgName= req.query.orgName
    let body = await parseJsonDataToNewLineDelimited(`${orgName}-score.json`,orgName)
    console.log(body)
    res.send(body).end()
})
// [
//     {
//       challengeCode: 'bph-001-a',
//       id: 'VpIC2UPM5F9v6ouTDp7W',
//       answers: [
//         [Object], [Object],
//         [Object], [Object],
//         [Object], [Object],
//         [Object]
//       ],
//       timestamp: 1577357308741,
//       subjectCode: 'bph',
//       employeeId: 123499,
//       orgName: 'bnp',
//       collection: '2019-12',
//       tracktime: '26/12/2019, 5:48:28 PM',
//       chapterCode: 'bph-001'
//     },
//     {
//       id: 'mZU09QYBP3yfKFN9EXk2',
//       answers: [
//         [Object], [Object],
//         [Object], [Object],
//         [Object], [Object],
//         [Object]
//       ],
//       timestamp: 1577353091433,
//       subjectCode: 'bph',
//       employeeId: 123499,
//       orgName: 'bnp',
//       collection: '2019-12',
//       tracktime: '26/12/2019, 4:38:11 PM',
//       chapterCode: 'bph-001',
//       challengeCode: 'bph-001-a'
//     },
//     playIndex: NaN,
//     roundIndex: NaN,
//     timestamp: 'undefined'
//   ]