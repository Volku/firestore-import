const FileSystem = require('fs')
const ndjson = require('ndjson')

const writeFile = (filename, str) => {

    FileSystem.writeFile(filename, str, (err) => {
        if (err) throw err;
    });
}

const parseJsonDataToNewLineDelimited = (filename) => {
    let rawData = FileSystem.readFileSync(filename);
    rawData = JSON.parse(rawData)
    dirname = 'NDJSON-SCORE'
    let transformStream =ndjson.stringify();
    let outputStream =transformStream.pipe( FileSystem.createWriteStream(dirname+"/old-tcp-score.json"))

    rawData.forEach(
        function iterator(data){
            transformStream.write( data);
        }
    )

    transformStream.end();

    outputStream.on(
        "finish",
        function handleFinish() {
    
            console.log( "ndjson serialization complete!"  );
            console.log( "- - - - - - - - - - - - - - - - - - - - - - -" );
    
        }
    );
}

module.exports = {
    writeFile:writeFile,
    parseJsonDataToNewLineDelimited:parseJsonDataToNewLineDelimited,
}