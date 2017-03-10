"use strict";

var mongoose = require('mongoose');
var Schema = require('mongoose').Schema;
var moment = require('moment');
var AWS = require('aws-sdk');
var s3 = new AWS.S3({ apiVersion: '2006-03-01' });

var testLogSchema = new Schema({ 
    fileName: { type: String, unique: true },
    taskName: { type: String },
    fileUrl: {type: String},
    objectId: { type: String},
    dateCreated: { type: Date },
    errorCount: { type: Number }
}, { versionKey: false });

var jobSchema = new Schema({ 
  objectId: String,
  taskName: String
}, { versionKey: false });

exports.handler = (event, context) => {
  console.log(event.Records[0].s3.object.key.toString());
  var db = mongoose.connect('mongodb://ec2-52-91-110-103.compute-1.amazonaws.com:27017/logFiles');

  var Log = db.model('testlog', testLogSchema);
  var Job = db.model('job', jobSchema);

  var pattern = /^(sf-errorlogs\/)[0-9]{2}[-][0-9]{2}[-][0-9]{4}(\/)/;
  var filename = event.Records[0].s3.object.key.toString();
  var url = 'https://s3.amazonaws.com/informatica-logs/';

  /* TODO: Convert everything to async/await */

  s3.getObject({ Bucket: event.Records[0].s3.bucket.name.toString(), Key: event.Records[0].s3.object.key.toString() }, (err, data) => {
      
    if (err) {
        console.log(err, err.stack);
        db.connection.close();
    } else {
        Job.findOne({ "objectId": filename.replace(pattern, '').substring(6, 26) })
        .then(job => {
            var newLog = new Log({
                fileName: filename.replace(pattern, ''),
                taskName: job.taskName,
                fileUrl: url + filename,
                objectId: filename.replace(pattern, '').substring(6, 26),
                dateCreated: moment(data.Metadata.lastwritetime, 'DD-MM-YYYY-HH:mm:ss').add(5, 'hours'),
                errorCount: (data.Body.toString('utf8').split('\n').length - 1) > 0 ? (data.Body.toString('utf8').split('\n').length - 1) : 0
            });
            newLog.save();
            db.connection.close();
        })
        .catch(err => {
            console.log(err.toString());
            db.connection.close();
        });
    }  
  });
};
