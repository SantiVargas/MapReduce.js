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

//Set flag
app.locals.partitioned = false;

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
  //Get the key value pairs from the input
  var kvpairs = req.body;
  //Predistribute the mapper pairs
  var kvChunks = evenSequentialDistributer(numberOfMappers, kvpairs);
  //Store the input across requests in app locals
  //ToDo: This should be stored in some other storage mechanism
  setMapperPairs(kvChunks);
  console.log(getMapperPairs())
  return res.send("Recieved Input");
});

//Serve Mapper Input
app.get('/mapperInput', function(req, res) {
  //Get all key value pairs
  var kvpairs = getMapperPairs();
  //Get one key value from the list and add default values of null
  //ToDo: Create algorithm to dynamically adjust how many kvpairs are served
  var kv = kvpairs.shift();
  var result = kv ? kv : [];
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
  var kvPairs = req.body;
  //Store the input across requests in app locals... This should be stored in some other storage mechanism
  addToReducerPairs(kvPairs);
  //ToDo: Perform some partitioning work here. Do it in the background
  return res.send("Recieved Input");
});

//Taken from https://github.com/jschanker/simplified-mapreduce-wordcount/blob/master/wordcount.js
function flatten(itemList) {
  return itemList.reduce(function(arr, list) {
    return arr.concat(list);
  });
}

function combiner(kVPairList) {
  return flatten(kVPairList);
}

//Taken from https://github.com/jschanker/simplified-mapreduce-wordcount/blob/master/wordcount.js
function sort(kVPairList) {
  kVPairList.sort(function(kVPair1, kVPair2) {
    var val = 0;
    if (kVPair1.key < kVPair2.key) {
      val = -1;
    } else if (kVPair1.key > kVPair2.key) {
      val = 1;
    }

    return val;
  });
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

//Serve Reducer Input
//ToDo: Move partitioning to the mapperOutput endpoint. Partition right after recieving input
app.get('/reducerInput', function(req, res) {
  if (!app.locals.partitioned) {
    //Partition Stuff Before Reducing
    //Combiner
    var unCombinedPairs = getReducerPairs();
    var kvPairs = combiner(unCombinedPairs);
    //Sorter
    sort(kvPairs);
    app.locals.partitioned = true;
    setReducerPairs(naiveModuloPartitioner(kvPairs, numberOfReducers));
  }
  var result = getReducerPairs();
  //Return the kvPairs
  return res.json(result);
});

//Master with push capability
//ToDo

app.listen(3000, function() {
  console.log('Example app listening on port 3000!');
});