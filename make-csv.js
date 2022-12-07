const converter = require('json-2-csv');
const fs = require('fs');

const results = JSON.parse(fs.readFileSync('results.json'));

converter.json2csv(results, (err, csv) => {
    if (err) {
        throw err;
    }
    console.log(csv);
    fs.writeFileSync('results.csv', csv);
})