var Model;
//Storage Variables
var rootStorage;
var mapperDataKey = 'mapperPairs';
var reducerDataKey = 'reducerPairs';
var finalDataKey = 'finalPairs';
var jobIdKey = 'jobId';
var mapperChunksCountKey = 'mapperChunksCount';
var mapperOutputReceivedCountKey = 'mapperOutputReceivedCount';
var reducerChunksCountKey = 'reducerChunksCount';
var reducerOutputReceivedCountKey = 'reducerOutputReceivedCount';
var mapperDataDefault = [];
var reducerDataDefault = function () {
  return {
    keys: {},
    partitions: []
  };
};
var finalDataDefault = [];
var jobIdDefault = undefined;
var countDefaults = 0;

module.exports = Model = function (rootStorageVariable) {
  rootStorage = rootStorageVariable;
  return Model;
};

function setStorage(key, value) {
  rootStorage[key] = value;
}

function getStorage(key) {
  return rootStorage[key];
}

Model.setMapperChunksCount = function (count) {
  setStorage(mapperChunksCountKey, count);
};

Model.setMapperOutputReceivedCount = function (count) {
  setStorage(mapperOutputReceivedCountKey, count);
};

Model.setReducerChunksCount = function (count) {
  setStorage(reducerChunksCountKey, count);
};

Model.setReducerOutputReceivedCount = function (count) {
  setStorage(reducerOutputReceivedCountKey, count);
};

Model.getMapperChunksCount = function () {
  return getStorage(mapperChunksCountKey);
};

Model.getMapperOutputReceivedCount = function () {
  return getStorage(mapperOutputReceivedCountKey);
};

Model.getReducerChunksCount = function () {
  return getStorage(reducerChunksCountKey);
};

Model.getReducerOutputReceivedCount = function () {
  return getStorage(reducerOutputReceivedCountKey);
};

Model.setJobId = function (id) {
  setStorage(jobIdKey, id);
};

Model.getJobId = function () {
  return getStorage(jobIdKey);
};

Model.setMapperPairs = function (data) {
  setStorage(mapperDataKey, data);
};

Model.getMapperPairs = function () {
  return getStorage(mapperDataKey) || mapperDataDefault;
};

Model.setReducerData = function (data) {
  setStorage(reducerDataKey, data);
};

Model.getReducerData = function () {
  return getStorage(reducerDataKey) || reducerDataDefault;
};

Model.getReducerPartitions = function () {
  return Model.getReducerData().partitions;
};

Model.setFinalData = function (data) {
  setStorage(finalDataKey, data);
};

Model.getFinalData = function () {
  return getStorage(finalDataKey) || finalDataDefault;
};

Model.resetStorageValues = function () {
  //Clear job id
  Model.setJobId(jobIdDefault);
  //Clear counts
  Model.setMapperChunksCount(countDefaults);
  Model.setMapperOutputReceivedCount(countDefaults);
  Model.setReducerChunksCount(countDefaults);
  Model.setReducerOutputReceivedCount(countDefaults);
  //Clear stored mapper data
  Model.setMapperPairs(mapperDataDefault);
  //Clear reducer data
  Model.setReducerData(reducerDataDefault());
  //Clear final data
  Model.setFinalData(finalDataDefault);
};