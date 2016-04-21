$(document).ready(function() {

  var mapReduceInputButtonId = "#mapinputbutton";
  var mapReduceInputTextAreaId = "#mapinput";
  var mapReduceInputURL = "http://localhost:3000/mapperInput";

  $(mapReduceInputButtonId).click(function() {
    //Submit the MapReduce Input
    alert("You clicked me!");
    var mapReduceInput = $(mapReduceInputTextAreaId).val();
    $.post({
        url: mapReduceInputURL,
        contentType: "application/json; charset=utf-8",
        data: mapReduceInput
      })
      .done(function(data, status) {
        alert("Data: " + data + " Status: " + status);
      })
      .fail(function(req, status) {
        alert("Fail");
        console.log(req.toString());
        console.log(status);
      });
  });

});