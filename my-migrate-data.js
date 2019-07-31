const mongodb = require('mongodb');
const async = require('async');
const customerData = require('./data/m3-customer-data.json');
const customerAddresses = require('./data/m3-customer-address-data.json');
const numOfRecordsPerLoop = parseInt(process.argv[2], 10) || customerData.length;

const url = 'mongodb://localhost:27017/edx-course-db';


let tasks = [];


mongodb.MongoClient.connect(url, (error, db) => {
    if (error) return process.exit(1);
    console.log('connected to database');

    //---------------------------------------------------------------------------------------------
    customerData.forEach((customer, index, list) => {
        // console.log(`index=${index}`);
        let startIndex = 0;
        let endIndex = 0;
        customerData[index] = Object.assign(customer, customerAddresses[index]);

        if (index % numOfRecordsPerLoop == 0) {
            console.log(`---> index=${index}`);

            startIndex = index;
            if ((startIndex + numOfRecordsPerLoop) > customerData.length) {
                endIndex = customerData.length;// - 1;
            }
            else {
                endIndex = startIndex + numOfRecordsPerLoop;
            }
            //endIndex = ((startIndex + numOfRecordsPerLoop) > customerData.length) ? (customerData.length - 1) : (startIndex + numOfRecordsPerLoop);
            console.log(`-------------> loop= ${index} --> startIndex= ${startIndex} endIndex= ${endIndex}`);
            tasks.push((done) => {
                db.collection('test-merge').insert(customerData.slice(startIndex, endIndex), (error, result) => {
                    done(error, result);
                    console.log(`inserting task with records ${startIndex} - ${endIndex - 1} of ${customerData.length}`);
                });
            });

        }
    });



    //----------------------------------------------------------------------------------------------
    console.log(`Launching ${tasks.length} parallel task(s)`);
    const startTime = new Date();
    async.parallel(tasks, (error, results) => {
        if (error) console.error(error);
        const endTime = new Date();
        var timeDiff = endTime - startTime;
        console.log(`inserted ${results.length} tasks`);
        console.log(`Finished in ${timeDiff} secs`);
        db.close();
    });
});