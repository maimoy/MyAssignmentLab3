//const express = require('express');
//const logger = require('morgan');
//const errorhandler = require('errorhandler');
const mongodb = require('mongodb');
//const bodyParser = require('body-parser');
const customerData = require('./data/m3-customer-data.json');
const customerAddresses = require('./data/m3-customer-address-data.json');
const collectionName = 'testmerge';
const async = require('async');

const numOfRecordsPerLoop = process.argv[2] || customerData.length;

const url = 'mongodb://localhost:27017/edx-course-db';
const startTime = new Date();

var endTime = new Date();
var timeDiff = new Date();
var loops = 0;
var writtenCustomers = 0;
var tasks = []

mongodb.MongoClient.connect(url, (error, db) => {
    if (error) return process.exit(1);
    console.log('connected to database');

    var customersChunkToWrite = [];
    var arrayTest = [];

    for (let index = 0; index < customerData.length; index++) {
        let currentCustomer = customerData[index];
        let dataToMerge = customerAddresses[index];
        currentCustomer = Object.assign(currentCustomer, dataToMerge);
        console.log(`customer with index ${index} and id=${currentCustomer.id} ready`)

        customersChunkToWrite.push(currentCustomer);

        if (customersChunkToWrite.length >= numOfRecordsPerLoop) {
            loops++;
            console.log(`customersChunkToWrite = ${customersChunkToWrite.length}`);
            console.log(`do insert Many loop ${loops} index=${index} `);
            //arrayTest = customersChunkToWrite.slice(0);
            tasks.push((callback) => {

                db.collection(collectionName).insert(arrayTest, (error, results) => {
                    if (error) return callback(error);
                    callback(error, results);
                    console.log(`${result.toArray.length} customers written to collection`);
                });
            });

            writtenCustomers += customersChunkToWrite.length;
            customersChunkToWrite = [];
            arrayTest = [];
            console.log(`writtenCustomers=${writtenCustomers}`);
        }


    }
    /*
        if (writtenCustomers < customerData.length) {
            tasks.push((callback) => {
                console.log(`write rest ${customersChunkToWrite.length}`);
                db.collection(collectionName).insertMany(customersChunkToWrite, (error, results) => {
                    if (error) callback(error);
                    //console.log(`${result.toArray.length} customers written to collection`);
                });
            });
        }  // if
    */
    async.parallel(tasks, (error, results) => {
        if (error) console.error(error);
        console.log('Done!!!');
        endTime = new Date();
        timeDiff = endTime - startTime;  //time in milliseconds
        timeDiff = timeDiff / 1000;
        console.log(`Finished in ${timeDiff} secs`);
        db.close();
    });

    //endTime = new Date();
    // timeDiff = endTime - startTime;  //time in milliseconds
    // timeDiff = timeDiff / 1000;
    // console.log(`Finished in ${timeDiff} secs`);

    //db.close();
    //return;
});




