function mapper(fileNameLinePairs) {
  // filenameLinePairs consists of a list of objects in the form {key: fileName, value: line}
  var result = fileNameLinePairs.map(function (kVPair) {
    var fileName = kVPair.key; // ignored for output from this map function
    var line = kVPair.value;
    words = line.split(" ");
    return words.map(function (word) {
      return { key: word, value: "1" }; // each word contributes 1 to the total
    });
  });

  function flatten(itemList) {
    return itemList.reduce(function (arr, list) {
      return arr.concat(list);
    });
  }

  return flatten(result);
}

function reducer(list) {
  function sort(kVPairList) {
    kVPairList.sort(function (kVPair1, kVPair2) {
      var val = 0;
      if (kVPair1.key < kVPair2.key) {
        val = -1;
      }
      else if (kVPair1.key > kVPair2.key) {
        val = 1;
      }

      return val;
    });
  }
  sort(list);

  function plus(a, b) {
    return a + parseInt(b);
  }

  function reduce(sortedKeyValuePairs) {
    var i = 0;
    var keyValuesList = [];
    while (i < sortedKeyValuePairs.length) {
      var key = sortedKeyValuePairs[i].key;
      var singleKeyValuesList = { key: key, values: [] };
      while (i < sortedKeyValuePairs.length && key === sortedKeyValuePairs[i].key) {
        singleKeyValuesList.values.push(sortedKeyValuePairs[i].value);
        i++;
      }
      keyValuesList.push({
        key: singleKeyValuesList.key,
        value: singleKeyValuesList.values.reduce(plus, 0)
      });
    }

    return keyValuesList;
    /*	
      var singleKeyValuesList = {key: sortedKeyValuePairs[0].key, value:sortedKeyValuePairs[0].key};
      sortedKeyValuePairs.forEach(function(kVPair) {
        if(singleKeyValuesList.key != kVPair)
        singleKeyValuePairs.push()
      });
    */
  }
  return reduce(list);
}