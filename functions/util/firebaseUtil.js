const admin = require('firebase-admin')
admin.initializeApp({
    credential: admin.credential.applicationDefault()
})

const db = admin.firestore();




const getScoreFromLog = async (orgName, month) => {
    let result = []
    await db.collection(`${orgName}-log`).doc('score').collection(month).get().then(snapshot => {
        snapshot.forEach(doc => {
            let temp = doc.data()
            temp['id'] = doc.id
            result.push(temp)
        })
        return
    }).catch(err => {
        console.log('error emited: ' + err)
    })
    return result
}

const getCollectionList = async (orgName) => {
    let col_list = []
    await db.collection(`${orgName}-log`).doc('score').listCollections().then(collections => {
        collections.forEach(collection => {
            col_list.push(collection.id)
        });
    });
    return col_list
}

module.exports ={
    getScoreFromLog:getScoreFromLog,
    getCollectionList:getCollectionList
}