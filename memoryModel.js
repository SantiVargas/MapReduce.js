var Model;
//Storage Variables
var rootStorage;
var mapperDataKey = 'mapperPairs';
var reducerDataKey = 'reducerPairs';
var finalDataKey = 'finalPairs';
var mapperDataDefault = [];
var reducerDataDefault = {
  keys: {},
  partitions: []
};
var finalDataDefault = [];

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
  //Clear stored mapper data
  Model.setMapperPairs(mapperDataDefault);
  //Clear reducer data
  Model.setReducerData(reducerDataDefault);
  //Clear final data
  Model.setFinalData(finalDataDefault);
};