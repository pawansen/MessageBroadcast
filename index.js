const express = require('express');
const app = express();
const dotenv = require('dotenv')
const bodyparser = require('body-parser')
const http = require('http');
const fs = require('fs');
const { parse } = require('csv-parse');
const { MongoClient } = require('mongodb');
const axios = require('axios');
dotenv.config()
const db = require('./db');
const commentsSchema = require('./comments.model')
const port = process.env.PORT;
const host = process.env.HOST;
// Body-parser middleware
app.use(bodyparser.urlencoded({ extended: true }))
app.use(bodyparser.json())
/**
 * 
 * populate comments and store in DB
 * 
 */
app.get('/populate', async function (req, res) {
    console.time('Reading json')
    /** insert comments */
    const fileUrl = 'http://cfte.mbwebportal.com/deepak/csvdata.csv';
    const destination = './uploads/csvdata.csv';
    var options = {
        'method': 'GET',
        'url': 'https://jsonplaceholder.typicode.com/comments'
    };
    await axios(options).then((comments) => {
        var result = comments.data;
        commentsSchema.insertMany(result).then((insert) => {
            console.log('inserted')
        }).catch((err) => {
            console.log(err)
        })
    }).then((err) => {
        console.log(err)
    })

    /** download csv and import in db */
    const file = fs.createWriteStream(destination);
    await http.get(fileUrl, (response) => {
        response.pipe(file);
        file.on('finish', () => {
            file.close(() => {
                var csvData = [];
                fs.createReadStream(destination)
                    .pipe(parse())
                    .on('data', (data) => csvData.push(data))
                    .on('end', () => {
                        commentsSchema.insertMany(csvData).then((insert) => {
                            console.log('inserted')
                            //res.send('inserted');
                        }).catch((err) => {
                            console.log(err)
                        })
                    });
                // res.send(200, { message: 'All Comments Inserted!' });
            });
        });
    }).on('error', (err) => {
        fs.unlink(destination, () => {
            console.error('Error downloading file:', err);
        });
    });
    /** download big csv and import in db */
    const bigFileUrl = 'http://cfte.mbwebportal.com/deepak/bigcsvdata.csv';
    const bigFileDestination = './uploads/bigcsvdata.csv';
    const files = fs.createWriteStream(bigFileDestination);
    await http.get(bigFileUrl, (response) => {
        response.pipe(files);
        files.on('finish', () => {
            files.close(() => {
                var csvDatas = [];
                fs.createReadStream(bigFileDestination)
                    .pipe(parse())
                    .on('data', (data) => csvDatas.push(data))
                    .on('end', () => {
                        console.time('Inserting records')
                        commentsSchema.insertMany(csvDatas).then((insert) => {
                            console.timeEnd('Inserting records')
                            res.send('All Comments Inserted!');
                        }).catch((err) => {
                            console.log(err)
                        })

                    });
            });
        });
    }).on('error', (err) => {
        fs.unlink(destination, () => {
            console.error('Error downloading file:', err);
        });
    });

})

/**
 * 
 * Fetch data in comments
 * 
 */
app.post('/search', async function (req, res) {
    let { name, email, body, limit, pageNo, sortBy, sortType } = req.body;
    let skip = (pageNo) ? Number(pageNo) * Number(limit) : 0;
    console.log(skip)
    let where = {};
    if (name) {
        where.name = { '$regex': name, '$options': 'i' }
    }
    if (email) {
        where.email = { '$regex': email, '$options': 'i' }
    }
    if (body) {
        where.body = { '$regex': body, '$options': 'i' }
    }
    console.log(where)
    commentsSchema.find(where).sort({ sortBy: parseInt(sortType) }).limit(Number(limit)).skip(Number(skip)).then((comments) => {
        res.status(200).send({ message: 'Listed!', data: comments })
    }).catch((err) => {
        console.log(err)
    })

})
app.listen(port, () => {
    console.log(`MessageBroadcast Running on http://${host}:${port}`)
})