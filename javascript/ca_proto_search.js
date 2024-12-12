//---------------------------------------------------------------------
// Copyright 2024 Canadian Light Source, Inc. All rights reserved
//     - see LICENSE.md for limitations on use.
//
// Description:
//     TODO: <<Insert basic description of the purpose and use.>>
//---------------------------------------------------------------------
$(document).ready(caProtoSearch);

function caProtoSearch() {
  $.ajaxSetup({ cache: false });
  $(".pv_index").each(function () {
    var pv_index = $(this).attr("value");
    // console.log("the pv_index" + pv_index);

    var pv_name_id = pv_index + "_pv_name_id";
    // console.log("the pv_name_id =" + pv_name_id);
    pv_name = document.getElementById(pv_name_id).innerHTML;
    // console.log("the pv_name =" + pv_name);

    var ioc_id_id = pv_index + "_ioc_id";
    // console.log("the ioc_id_id =" + ioc_id_id);
    ioc_id = document.getElementById(ioc_id_id).innerHTML;
    // console.log("the ioc_id =" + ioc_id);

    var port_id = pv_index + "_port_id";
    // console.log("the port_id =" + port_id);
    port = document.getElementById(port_id).innerHTML;
    // console.log("the port =" + port);

    var last_seen_id = pv_index + "_last_seen_id";
    // console.log("the last_seen_id =" + last_seen_id);
    last_seen = document.getElementById(last_seen_id).innerHTML;
    // console.log("the last_seen =" + last_seen);

    var url = new URL("http://%s:%d/casearch");
    url.searchParams.set("pv_name", pv_name);
    url.searchParams.append("index", pv_index);
    url.searchParams.append("ioc_id", ioc_id);
    url.searchParams.append("port", port);
    url.searchParams.append("last_seen", last_seen);
    url.searchParams.append("web_server", "%s");
    url.searchParams.append("web_port", "%d");

    var http_request = new XMLHttpRequest();
    try {
      http_request = new XMLHttpRequest();
    } catch (e) {
      try {
        http_request = new ActiveXObject("Msxml2.XMLHTTP");
      } catch (e) {
        try {
          http_request = new ActiveXObject("Microsoft.XMLHTTP");
        } catch (e) {
          // Something went wrong
          alert("Your browser broke!");
          return false;
        }
      }
    }

    http_request.onreadystatechange = function () {
      if (http_request.readyState == 4 && http_request.status == 200) {
        // Javascript function JSON.parse to parse JSON data
        console.log("response" + http_request.responseText);
        var jsonObj = JSON.parse(http_request.responseText);

        if (jsonObj.hasOwnProperty("server_id")) {
          element_id = jsonObj.server_id;
          document.getElementById(element_id).innerHTML = jsonObj.server_val;
        }

        if (jsonObj.hasOwnProperty("port_id")) {
          element_id = jsonObj.port_id;
          document.getElementById(element_id).innerHTML = jsonObj.port_val;
        }

        if (jsonObj.hasOwnProperty("last_seen_id")) {
          element_id = jsonObj.last_seen_id;
          document.getElementById(element_id).innerHTML = jsonObj.last_seen_val;
        }

        if (jsonObj.hasOwnProperty("status_id")) {
          element_id = jsonObj.status_id;
          document.getElementById(element_id).innerHTML = jsonObj.status_val;
        }
      }
    };
    http_request.open("GET", url, true);
    http_request.send();
  });
}
