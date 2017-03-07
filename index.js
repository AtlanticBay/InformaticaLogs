"use strict";

var mongoose = require('mongoose');
var Schema = require('mongoose').Schema;
var moment = require('moment');
var AWS = require('aws-sdk');
var s3 = new AWS.S3({ apiVersion: '2006-03-01' });

var testLogSchema = new Schema({ 
    fullPath: { type: String, unique: true },
    fileName: { type: String },
    taskName: { type: String },
    dateCreated: { type: Date },
    errorCount: { type: Number }
}, { versionKey: false });

var jobSchema = new Schema({ 
  objectId: String,
  taskName: String
}, { versionKey: false });

exports.handler = (event, context) => {

  var db = mongoose.connect('mongodb://ec2-52-91-110-103.compute-1.amazonaws.com:27017/logFiles');

  var Log = db.model('testlog', testLogSchema);
  var Job = db.model('job', jobSchema);
  
  var filename = `https://s3.amazonaws.com/informatica-logs/sf-errorlogs/${moment().format('DD[-]MM[-]YYYY')}/${event.Records[0].s3.object.key.toString()}`;

  s3.getObject({ Bucket: event.Records[0].s3.bucket.name.toString(), Key: event.Records[0].s3.object.key.toString() }, (err, data) => {
      
    if (err) {
        console.log(err, err.stack);
        db.connection.close();
    } else {
        Job.findOne({ "objectId": event.Records[0].s3.object.key.toString().substring(6, 26) })
        .then(job => {
            var newLog = new Log({
                fullPath: filename,
                fileName: event.Records[0].s3.object.key.toString(),
                taskName: job.taskName,
                dateCreated: moment(data.Metadata.lastwritetime, 'DD-MM-YYYY-HH:mm:ss').add(5, 'hours'),
                errorCount: (data.Body.toString('utf8').split('\n').length - 1)
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
