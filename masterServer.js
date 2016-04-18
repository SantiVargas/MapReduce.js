var express = require('express');
var bodyParser = require('body-parser');
var app = express();
var _ = require('lodash');
//Setup multer for file uploads
var multer = require('multer');
//Configuration Variables
var uploadDirectory = 'uploads/';

//Master without push capability
//Make the uploadDirectory a static directory so that it can be accessed publicly
app.use(express.static(uploadDirectory));
//Use the bodyParser
app.use(bodyParser.json());

//KVPairs storage
var mapperDataKey = 'mapperPairs';
var reducerDataKey = 'reducerPairs'

function setMapperPairs(kvList) {
  app.locals[mapperDataKey] = kvList;
}

function getMapperPairs() {
  return app.locals[mapperDataKey];
}

function getReducerPairs() {
  return app.locals[reducerDataKey];
}

function setReducerPairs(kvList) {
  app.locals[reducerDataKey] = kvList;
}

function addToReducerPairs(kvPairList) {
  //Get the value
  var pairs = getReducerPairs() || [];
  //Add the value
  pairs.push(kvPairList);
  //Save the new value
  setReducerPairs(pairs);
}

//Endpoint to upload a file by name and extension
app.post('/upload/:extension/:fileName', function(req, res, next) {
  var fileFieldName = 'uploadFile';

  //Apply the multer middleware 
  var genericFileUploader = multer({
    storage: multer.diskStorage({
      destination: uploadDirectory, //When using a string, multer will automatically create the directory for us
      filename: function(req, file, cb) {
        var fileName = req.params.fileName;
        var fileExtension = req.params.extension;
        cb(null, fileName + '.' + fileExtension);
      }
    })
  });
  genericFileUploader.single(fileFieldName)(req, res, next);
}, function(req, res) {
  //After uploading
  //Clear stored mapper data
  setMapperPairs([]);
  return res.send('Congrats');
});


//Sample JSON Input
// [{
//     "key": "hamlet.txt",
//     "value": "to be or not to be that is the question"
//   }, {
//     "key": "hamlet.txt",
//     "value": "whether tis nobler in the mind to suffer"
//   }, {
//     "key": "hamlet.txt",
//     "value": "the slings and arrows of outrageous fortune"
//   }, {
//     "key": "hamlet.txt",
//     "value": "or to take arms against a sea of troubles"
//   }, {
//     "key": "hamlet.txt",
//     "value": "and by opposing end them to die to sleep"
//   }, {
//     "key": "hamlet.txt",
//     "value": "no more and by a sleep to say we end"
//   }, {
//     "key": "hamlet.txt",
//     "value": "the heart-ache and the thousand natural shocks"
//   }]
//Get Input
app.post('/mapperInput', function(req, res) {
  console.log(req.body);
  //Get the key value pairs from the input
  var kvpairs = req.body;
  //Store the input across requests in app locals... This should be stored in some other storage mechanism
  setMapperPairs(kvpairs);
  return res.send("Recieved Input");
});

//Serve Mapper Input
app.get('/mapperInput', function(req, res) {
  //Get all key value pairs
  var kvpairs = getMapperPairs();
  //Get one key value from the list and add default values of null
  //ToDo: Create algorithm to dynamically adjust how many kvpairs are served
  var kv = _.defaults(kvpairs.shift(), {
    key: null,
    value: null
  });
  //Return the kvpair
  return res.json([kv]);
});

//Sample JSON Input
// [{
//   "key": "to",
//   "value": "1"
// }, {
//   "key": "be",
//   "value": "1"
// }, {
//   "key": "or",
//   "value": "1"
// }, {
//   "key": "not",
//   "value": "1"
// }, {
//   "key": "to",
//   "value": "1"
// }, {
//   "key": "be",
//   "value": "1"
// }, {
//   "key": "that",
//   "value": "1"
// }, {
//   "key": "is",
//   "value": "1"
// }, {
//   "key": "the",
//   "value": "1"
// }, {
//   "key": "question",
//   "value": "1"
// }]
//Recieve Mapper Output
app.post('/mapperOutput', function(req, res) {
  //Get the key value pairs from the input
  var kvPairs = req.body;
  //Store the input across requests in app locals... This should be stored in some other storage mechanism
  addToReducerPairs(kvPairs);
  return res.send("Recieved Input");
});

//Taken from https://github.com/jschanker/simplified-mapreduce-wordcount/blob/master/wordcount.js
function flatten(itemList) {
  return itemList.reduce(function(arr, list) {
    return arr.concat(list);  
  });
}

//Taken from https://github.com/jschanker/simplified-mapreduce-wordcount/blob/master/wordcount.js
function sort(kVPairList) {
  kVPairList.sort(function(kVPair1, kVPair2) {
    var val = 0;
    if(kVPair1.key < kVPair2.key) {
      val = -1;
    }
    else if(kVPair1.key > kVPair2.key) {
      val = 1;
    }
    
    return val;
  });
}

//Serve Reducer Input
//ToDo: Allow Parallelization
app.get('/reducerInput', function(req, res) {
  //Get all key value pairs
  var unflattenedPairs = getReducerPairs();
  var kvPairs = flatten(unflattenedPairs);
  console.log("Flattened Pairs", kvPairs);
  sort(kvPairs);
  console.log("Sorted Pairs", kvPairs);
  //ToDo: Section off the reducer pairs to allow for parallelization
  //Return the kvPairs
  return res.json(kvPairs);
});

//Master with push capability
//ToDo

app.listen(3000, function() {
  console.log('Example app listening on port 3000!');
});