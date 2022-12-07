const fs = require("fs");
const { parse } = require("csv-parse");
const { stringify } = require("csv-stringify");

let users = {};
let assets = {};
let csvArr = [];

fs.createReadStream("./data/users.csv")
    .pipe(parse({ delimiter: ",", from_line: 2}))
    .on("data", function (row) {
        users[row[0]] = row[4];
    })
    .on("end", function () {
        fs.createReadStream("./data/interactions.csv")
            .pipe(parse({ delimiter: ",", from_line: 2}))
            .on("data", function (row) {
                const aid = row[1];
                if (!assets[aid]) {
                    assets[aid] = {"male": 0, "female": 0, "unknownUsers": 0};
                }
                if (users[row[0]]!=1 && users[row[0]]!=0) {
                    assets[aid].unknownUsers += 1;
                }else{
                    if (users[row[0]] == 1) {
                        assets[aid].female += 1
                    }else{
                        assets[aid].male += 1;
                    }
                }
            })
            .on("end", function () {
                for (const id in assets) {
                    const sumEvents = assets[id].male + assets[id].female + assets[id].unknownUsers;
                    const sumKnown = assets[id].male + assets[id].female;
                    const perMale = assets[id].male/sumKnown * 100;
                    const perFemale = assets[id].female/sumKnown * 100;
                    const kindaSus = Math.abs(perMale - perFemale) < 10 ? 1 : 0;
                    csvArr.push({asset_id: id, male: assets[id].male, female: assets[id].female, unknownUsers: assets[id].unknownUsers, percentMale:`${Math.round(perMale)}%`, percentFemale: `${Math.round(perFemale)}%`, near_even: kindaSus, total_interactions: sumEvents});
                }
                const sassets = JSON.stringify(csvArr);
                fs.writeFile('results.json', sassets, 'utf8', function (err) {
                    if (err) throw err;
                    console.log('wrote to json file');
                });
              })
              .on("error", function (error) {
                console.log(error.message);
              });
    })
    .on("error", function () {
        console.log(error.message);
    });