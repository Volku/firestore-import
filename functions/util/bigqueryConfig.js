const { BigQuery } = require('@google-cloud/bigquery')


const region = 'asia-northeast1'

const runtimeOpts = {
    timeoutSeconds: 300,
    memory: '2GB'
}
let bigquery = new BigQuery({ projectId: 'vondercenter' })

module.exports = {
    region:region,
    runtimeOpts:runtimeOpts,
    bigquery:bigquery
}