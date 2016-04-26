$(document).ready(function () {

  var mapReduceInputButtonId = "#mapinputbutton";
  var mapReduceInputTextAreaId = "#mapinput";
  var mapReduceURLBase = "http://localhost:3000";
  var mapReduceMapperInputURL = mapReduceURLBase + "/mapperInput";
  var mapReduceMapperOutputURL = mapReduceURLBase + "/mapperOutput";
  var mapReduceReducerInputURL = mapReduceURLBase + "/reducerInput";
  var mapReduceReducerOutputURL = mapReduceURLBase + "/reducerOutput";
  var mapperCodeTextAreaId = "#mapcode";
  var mapperCodeButtonId = "#mapcodebutton";
  var reducerCodeTextAreaId = "#reducercode";
  var reducerCodeButtonId = "#reducercodebutton";
  var outputButtonId = "#outputbutton";
  var outputAreaId = "#outputarea";
  var mapperOutputAreaId = "#mapperoutput";
  var reducerOutputAreaId = "#reduceroutput";

  $(mapReduceInputButtonId).click(function () {
    var mapReduceInput = $(mapReduceInputTextAreaId).val();
    $.post({
      url: mapReduceMapperInputURL,
      contentType: "application/json; charset=utf-8",
      data: mapReduceInput
    })
      .fail(function (req, status) {
        alert("Map Reduce Input Post Failure");
        console.log(req.toString());
        console.log(status);
      });
  });

  var mapperFunction = function () {
    alert("No Function Specified");
  };

  $(mapperCodeButtonId).click(function () {
    var mapperCode = $(mapperCodeTextAreaId).val();
    mapperFunction = eval("(" + mapperCode + ")");
    $.get({
      url: mapReduceMapperInputURL
    })
      .done(function (data, status) {
        console.log("D", JSON.stringify(data))
        var result = mapperFunction(data);
        console.log("R", JSON.stringify(result))
        $(mapperOutputAreaId).text(JSON.stringify(result));

        $.post({
          url: mapReduceMapperOutputURL,
          contentType: "application/json; charset=utf-8",
          data: JSON.stringify(result)
        })
          .fail(function (req, status) {
            alert("Map Reduce Mapper Output Post Failure");
            console.log(JSON.stringify(result));
            console.log(status);
          });
      })
      .fail(function (req, status) {
        alert("Map Reduce Mapper Input Request Failure");
        console.log(req.toString());
        console.log(status);
      });
  });

  $(reducerCodeButtonId).click(function () {
    var reducerCode = $(reducerCodeTextAreaId).val();
    reducerFunction = eval("(" + reducerCode + ")");
    $.get({
      url: mapReduceReducerInputURL
    })
      .done(function (data, status) {
        console.log("Data", JSON.stringify(data))
        var result = reducerFunction(data);
        $(reducerOutputAreaId).text(JSON.stringify(result));

        $.post({
          url: mapReduceReducerOutputURL,
          contentType: "application/json; charset=utf-8",
          data: JSON.stringify(result)
        })
          .fail(function (req, status) {
            alert("Map Reduce Reducer Output Post Failure");
            console.log(JSON.stringify(result));
            console.log(status);
          });
      })
      .fail(function (req, status) {
        alert("Map Reduce Reducer Input Request Failure");
        console.log(req.toString());
        console.log(status);
      });
  });

  $(outputButtonId).click(function () {
    $.get({
      url: mapReduceReducerOutputURL
    })
      .done(function (data, status) {
        $(outputAreaId).text(JSON.stringify(data));
      })
      .fail(function (req, status) {
        alert("Map Reduce Reducer Output Request Failure");
        console.log(req.toString());
        console.log(status);
      });
  });

});