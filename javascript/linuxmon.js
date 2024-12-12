//---------------------------------------------------------------------
// Copyright 2024 Canadian Light Source, Inc. All rights reserved
//     - see LICENSE.md for limitations on use.
//
// Description:
//     TODO: <<Insert basic description of the purpose and use.>>
//---------------------------------------------------------------------
$(document).ready(getPvValues);

function getPvValues()
{
    $.ajaxSetup({ cache: false });
    $('.pv_value').each(function(){
        var pv_name = $(this).attr('item_id');
        var url = 'http://%s:%s/caget/' + pv_name
        $(this).text("Calling caget...");
        $(this).load(url);
    });
}