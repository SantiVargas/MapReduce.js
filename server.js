var express = require('express');
var bodyParser = require('body-parser');
var app = express();
var _ = require('lodash');
//Store Values across requests in app.locals
var mapReduceStorage = require('./memoryModel')(app.locals);
//Setup multer for file uploads
var multer = require('multer');
//Configuration Variables
//ToDo: Move these to a config file
var uploadDirectory = 'uploads/';
var numberOfMappers = 16;
var numberOfReducers = 16;

//Master without push capability
app.use(express.static('sample/'));
//Make the uploadDirectory a static directory so that it can be accessed publicly
app.use(express.static(uploadDirectory));
//Use the bodyParser
app.use(bodyParser.json());

//Allow Cross Origin Requests
app.use(function (req, res, next) {
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

//Combining, Partitioning, and Distributing Functions
function partitionCombiner(currentData, newPairs, numOfPartitions, callback) {
  //Get the value
  newPairs.forEach(function (current) {
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
  var groupedList = _.groupBy(kVPairList, function (obj) {
    return obj.key;
  });
  var partitionedList = [];
  var keys = Object.keys(groupedList);
  keys.forEach(function (value, index) {
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
  for (var i = 0, currentIndex = 0; length > currentIndex; i++) {
    var endingIndex = currentIndex + elementsPerChunk + (i < chunksWithRemainder);
    chunkedArray.push(array.slice(currentIndex, endingIndex));
    currentIndex = endingIndex;
  }
  return chunkedArray;
}

function concatCombiner(oldArray, newArray, callback) {
  var result = oldArray.concat(newArray);
  return callback(result);
}

//Endpoint to upload a file by name and extension
app.post('/upload/:extension/:fileName', function (req, res, next) {
  var fileFieldName = 'uploadFile';

  //Apply the multer middleware 
  var genericFileUploader = multer({
    storage: multer.diskStorage({
      destination: uploadDirectory, //When using a string, multer will automatically create the directory for us
      filename: function (req, file, cb) {
        var fileName = req.params.fileName;
        var fileExtension = req.params.extension;
        cb(null, fileName + '.' + fileExtension);
      }
    })
  });
  genericFileUploader.single(fileFieldName)(req, res, next);
}, function (req, res) {
  //After uploading
  return res.send('Congrats');
});

//Reset Storage
app.get('/reset', function (req, res) {
  //Reset storage to default values
  mapReduceStorage.resetStorageValues();
  //Send a success JSON response
  var response = {
    success: true
  };
  return res.json(response);
});

//ToDo: Endpoint to make output -> input and set id 

//Sample JSON Input
// {
// 	"data": [{
// 		"key": "hamlet.txt",
// 		"value": "to be or not to be that is the question"
// 	}, {
// 		"key": "hamlet.txt",
// 		"value": "whether tis nobler in the mind to suffer"
// 	}],
// 	"id": 1
// }
//Get Input
app.post('/mapperInput', function (req, res) {
  //Reset storage to default values
  mapReduceStorage.resetStorageValues();
  //Get the key value pairs from the input
  var kVPairs = req.body.data;
  //Get the job id from post body
  var jobId = req.body.id;
  //Predistribute the mapper pairs
  var kVChunks = evenSequentialDistributer(numberOfMappers, kVPairs);
  //Store the input
  mapReduceStorage.setMapperPairs(kVChunks);
  //Store size of mapperChunks
  var sizeOfMapperChunks = kVChunks.length;
  mapReduceStorage.setMapperChunksCount(sizeOfMapperChunks);
  //Store the job id
  mapReduceStorage.setJobId(jobId);
  //Send a success JSON response
  var response = {
    success: true,
    id: jobId
  };
  return res.json(response);
});

//Serve Mapper Input
app.get('/mapperInput', function (req, res) {
  //Get all key value pairs
  var kVPairs = mapReduceStorage.getMapperPairs();
  var kV = kVPairs.shift();
  var list = kV ? kV : [];
  //Get the job id and serve it with the output
  var jobId = mapReduceStorage.getJobId();
  //Return the list
  var result = {
    success: true,
    data: list,
    id: jobId
  };
  return res.json(result);
});

//Sample JSON Input
// {
// 	"data": [{
// 		"key": "to",
// 		"value": "1"
// 	}, {
// 		"key": "be",
// 		"value": "1"
// 	}, {
// 		"key": "or",
// 		"value": "1"
// 	}, {
// 		"key": "not",
// 		"value": "1"
// 	}, {
// 		"key": "to",
// 		"value": "1"
// 	}, {
// 		"key": "be",
// 		"value": "1"
// 	}]
// }
//Receive Mapper Output
app.post('/mapperOutput', function (req, res) {
  //Get the key value pairs from the input
  var kVPairs = req.body.data;
  //Increase and save the mapperOutputReceivedCount
  var currentCount = mapReduceStorage.getMapperOutputReceivedCount();
  currentCount++;
  mapReduceStorage.setMapperOutputReceivedCount(currentCount);
  //Store the input
  //ToDo: Ensure that multiple calls does not lose data... Hmm, reminds me of message queues and event driven stuff
  partitionCombiner(mapReduceStorage.getReducerData(), kVPairs, numberOfReducers, function (data) {
    var mapperOutputSent = mapReduceStorage.getMapperChunksCount();
    //If received the last mapperOutput then set the reducerChunksCount
    if (mapperOutputSent === currentCount) {
      mapReduceStorage.setReducerChunksCount(data.partitions.length);
    }
    mapReduceStorage.setReducerData(data);
  });
  //Send a success JSON response
  var response = {
    success: true
  };
  return res.json(response);
});

//Serve Reducer Input
app.get('/reducerInput', function (req, res) {
  //Get the job id and serve it with the output
  var jobId = mapReduceStorage.getJobId();
  var result = {
    success: false,
    data: [],
    id: jobId
  };
  //Check to see if all the mapper output was received 
  var mapperOutputSent = mapReduceStorage.getMapperChunksCount();
  var mapperOutputReceived = mapReduceStorage.getMapperOutputReceivedCount();
  if (mapperOutputSent === mapperOutputReceived) {
    var partitions = mapReduceStorage.getReducerPartitions();
    var firstPartition = partitions.shift();
    var partition = firstPartition ? firstPartition : [];

    //Populate the results
    result.success = true;
    result.data = partition;
  }
  return res.json(result);
});

//Receive Reducer Output
app.post('/reducerOutput', function (req, res) {
  //Get the key value pairs from the input
  var kVPairs = req.body;
  //Append the reducer data
  //ToDo: Ensure that multiple calls does not lose data... Hmm, reminds me of message queues and event driven stuff
  concatCombiner(mapReduceStorage.getFinalData(), kVPairs, mapReduceStorage.setFinalData);
  //Increase and save the reducer output received count
  var currentCount = mapReduceStorage.getReducerOutputReceivedCount();
  currentCount++;
  mapReduceStorage.setReducerOutputReceivedCount(currentCount);
  //Send a success JSON response
  var response = {
    success: true
  };
  return res.json(response);
});

//Show Reducer Output
app.get('/reducerOutput', function (req, res) {
  //Get the job id and serve it with the output
  var jobId = mapReduceStorage.getJobId();
  //Result when reducing is not finished
  var result = {
    success: false,
    data: [],
    id: jobId
  };
  //Check to see if all the reducer output was received 
  var reducerOutputSent = mapReduceStorage.getReducerChunksCount();
  var reducerOutputReceived = mapReduceStorage.getReducerOutputReceivedCount();
  if (reducerOutputSent === reducerOutputReceived) {
    var finalData = mapReduceStorage.getFinalData();
    //Return the final list
    result.success = true;
    result.data = finalData;
  }
  return res.json(result);
});

//Master with push capability
//ToDo

app.listen(3000, function () {
  console.log('Example app listening on port 3000!');
});