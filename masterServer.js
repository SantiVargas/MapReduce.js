var express = require('express');
var bodyParser = require('body-parser');
var app = express();
var _ = require('lodash');
//Setup multer for file uploads
var multer = require('multer');
//Configuration Variables
var uploadDirectory = 'uploads/';
var numberOfMappers = 16;
var numberOfReducers = 16;

//Master without push capability
//Make the uploadDirectory a static directory so that it can be accessed publicly
app.use(express.static(uploadDirectory));
//Use the bodyParser
app.use(bodyParser.json());

//Allow Cross Origin Requests
app.use(function(req, res, next) {
  res.header("Access-Control-Allow-Origin", "*");
  res.header("Access-Control-Allow-Headers", "Origin, X-Requested-With, Content-Type, Accept");
  next();
});

//Steps
//Input
//Distributer
//Mapper
//Combiner
//Sorter (Comparison)
//Partitioner
//Reducer
//Output

//App.Locals Values/Functions
//ToDo: Keys could be stored in some other storage mechanism
function setLocals(key, value) {
  app.locals[key] = value;
}

function getLocals(key) {
  return app.locals[key];
}

//Storage Values/Functions
var mapperDataDefault = [];
var reducerDataDefault = {
  keys: {},
  partitions: []
};
var mapperDataKey = 'mapperPairs';
var reducerDataKey = 'reducerPairs';

function setMapperPairs(kvList) {
  setLocals(mapperDataKey, kvList);
}

function getMapperPairs() {
  return getLocals(apperDataKey);
}

function setReducerData(kvList) {
  setLocals(reducerDataKey, kvList);
}

function getReducerData() {
  return getLocals(reducerDataKey);
}

function getReducerPartitions() {
  return getReducerData().partitions;
}

function resetStorageValues() {
  //Clear stored mapper data
  setMapperPairs(mapperDataDefault);
  //Clear reducer data
  setReducerData(reducerDataDefault);
}

//Combining, Partitioning, and Distributing Functions
function partitionCombiner(currentData, newPairs, numOfPartitions, callback) {
  currentData = currentData || reducerDataDefault;
  //Get the value
  newPairs.forEach(function(current) {
    //Partitioner
    var key = current.key;
    //Assign the key to a partition
    if (_.isUndefined(currentData.keys[key])) {
      var objKeys = Object.keys(currentData.keys);
      var index = objKeys.length % numOfPartitions;
      currentData.keys[key] = index;

      //Create the partition if it doesnt exist
      if (!currentData.partitions[index]) {
        currentData.partitions[index] = [];
      }
    }
    //Save the data to its partition
    var partitionNumber = currentData.keys[key];
    currentData.partitions[partitionNumber].push(current);
  });
  return callback(currentData);
}

function naiveModuloPartitioner(kVPairList, numOfChunks) {
  var groupedList = _.groupBy(kVPairList, function(obj) {
    return obj.key;
  });
  var partitionedList = [];
  var keys = Object.keys(groupedList);
  keys.forEach(function(value, index) {
    var newIndex = index % numOfChunks;
    if (!partitionedList[newIndex]) {
      partitionedList[newIndex] = [];
    }

    partitionedList[newIndex] = partitionedList[newIndex].concat(groupedList[value]);
  });
  return partitionedList;
}

function evenSequentialDistributer(numOfChunks, array) {
  var length = array.length;
  var elementsPerChunk = Math.floor(length / numOfChunks);
  var chunksWithRemainder = length % numOfChunks;
  var chunkedArray = [];
  for (var i = 0; i < numOfChunks; i++) {
    var startingIndex = elementsPerChunk * i;
    var endingIndex = startingIndex + elementsPerChunk + (i < chunksWithRemainder);
    chunkedArray.push(array.slice(startingIndex, endingIndex));
  }
  return chunkedArray;
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
  return res.send('Congrats');
});

//Sample JSON Input
// [{
//     "key": "hamlet.txt",
//     "value": "to be or not to be that is the question"
//   }, {
//     "key": "hamlet.txt",
//     "value": "whether tis nobler in the mind to suffer"
//   }]
//Get Input
app.post('/mapperInput', function(req, res) {
  //Reset storage to default values
  resetStorageValues();
  //Get the key value pairs from the input
  var kVPairs = req.body;
  //Predistribute the mapper pairs
  var kVChunks = evenSequentialDistributer(numberOfMappers, kVPairs);
  //Store the input
  setMapperPairs(kVChunks);
  return res.send("Recieved Input");
});

//Serve Mapper Input
app.get('/mapperInput', function(req, res) {
  //Get all key value pairs
  var kVPairs = getMapperPairs();
  var kV = kVPairs.shift();
  var result = kV ? kV : [];
  //Return the list
  return res.json(result);
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
// }]
//Recieve Mapper Output
app.post('/mapperOutput', function(req, res) {
  //Get the key value pairs from the input
  var kVPairs = req.body;
  //Store the input
  //ToDo: Ensure that multiple calls does not lose data... Hmm, reminds me of message queues and event driven stuff
  partitionCombiner(getReducerData(), kVPairs, numberOfReducers, setReducerData);
  return res.send("Recieved Input");
});

//Serve Reducer Input
app.get('/reducerInput', function(req, res) {
  var kVPairs = getReducerPartitions();
  var kV = kVPairs.shift();
  var result = kV ? kV : [];
  //Return the list
  return res.json(result);
});

//Master with push capability
//ToDo

app.listen(3000, function() {
  console.log('Example app listening on port 3000!');
});