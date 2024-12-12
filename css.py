# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
class CSS(object):

    IOC_GRID = """ 
<style>
table {
    border: 1px solid black;
    border-collapse: separate;
#    border-spacing: 3px;
    table-layout: fixed;
}

th, td {
    border: 1px solid black;
    border-collapse: separate;
    border-spacing: 3px;
    width: 2px;
    padding: 2px;
    font-size: 11px;
    text-align: center;
    white-space: nowrap;
    font-family: sans-serif;
}

.key_width {
    width: 75px;
    display: inline-block;
    font-size: 12px;
    text-align: center;
    font-family: sans-serif;
    white-space: nowrap;
}

.key_width_wide {
    width: 175px;
    display: inline-block;
    font-size: 12px;
    text-align: center;
    font-family: sans-serif;
    white-space: nowrap;
}

.title {
    font-size: 22px;
    font-weight: bold;
}

.no_break {
    white-space: nowrap;
}

</style>
"""

    IOC = """
<STYLE>
th {
    text-align: left;
}

th, td {
    padding-right: 10px;
    padding-left: 10px;
    font-size: 12px;
    white-space: nowrap;
    font-family: sans-serif;
    text-align: left;
}

td.right {
    text-align: right;
}

td.center {
    text-align: center;
}

th.right {
    text-align: right;
}

th.center {
    text-align: center;
}

tr:nth-child(even) {
    background: #EEE
}

tr:nth-child(odd) {
    background: #FFF
}

.my_monospace {
    font-family: "Courier New", Courier, sans-serif;
    white-space: pre;
    vertical-align: 5px;
}

.my_page_title {
    font-family: "Courier New", Courier, sans-serif;
    font-size: 30px;
    font-weight: bold;
    /* margin-: 10px; */
}

.inner
{
    display: inline-block;
}

.my_inline_button
{
    vertical-align: 5px;
}

</STYLE>
"""

    PV = """
<STYLE>
th {
    text-align: left;
}

th, td {
    padding-right: 10px;
    padding-left: 10px;
    font-size: 12px;
    white-space: nowrap;
    font-family: sans-serif;
    text-align: left;
}

td.right {
    text-align: right;
}

th.right {
    text-align: right;
}

tr:nth-child(even) {
    background: #EEE
}

tr:nth-child(odd) {
    background: #FFF
}

.my_monospace {
    font-family: "Courier New", Courier, sans-serif;
    white-space: pre;
}

img {
    max-width: none;
}
</STYLE>
"""

    EPICS_EVENTS = """
<style>
.good {
    border: 4px solid #11ff40;
    color: black;
    margin: 10px;
    padding: 10px;
    border-radius: 10px;
}

.bad {
    border: 4px solid #ff3220;
    color: black;
    margin: 10px;
    padding: 10px;
    border-radius: 10px;
}

.indent50 {
#    text-indent: 40px;
    margin-left: 40px;
}

.eventname {
    font-size: 24px;
    font-weight: normal;
    font-family: arial;
}

.iocname {
    font-size: 22px;
    font-weight: bold;
    font-family: "courier new";
}

.ab {
    position-left: 200px;
}

.timestamp {
    font-size: 12px;
    font-weight: bold;
    font-family: "courier new";
}

.column_title {
    font-size: 12px;
    float: left;
    width: 80px;
    font-family: arial;
}

.app_detail {
    font-size: 12px;
    font-weight: bold;
    font-family: "courier new";
}
</style>
"""

    TOOLTIP = """
<style type="text/css">

.tooltip {
  position: relative;
  display: inline-block;
  /* border-bottom: 1px dotted black; /* If you want dots under the hoverable text */ */
}

/* Tooltip text */
.tooltip .tooltiptext {
  visibility: hidden;
  width: 400px;
  background-color: #ffa; 
  /* backgound-color: #ffffaa; */
  color: #222;
  text-align: left;
  padding: 10px 10px;
  border: 1px solid;
  border-color: #111;
  border-radius: 3px;

  /* Position the tooltip text */
  position: absolute;
  z-index: 1;
  bottom: 125%;
  left: 50%;
  margin-left: -60px;
  /* margin-bottom: px; */


  /* Fade in tooltip */
  opacity: 0;
  transition: opacity 0.3s;
}
<!--
# /* Tooltip arrow */
# .tooltip .tooltiptext::after {
#   content: "";
#   position: absolute;
#   top: 100%;
#   left: 50%;
#   margin-left: -5px;
#   border-width: 5px;
#   border-style: solid;
#   border-color: #555 transparent transparent transparent;
# }
-->

/* Show the tooltip text when you mouse over the tooltip container */
.tooltip:hover .tooltiptext {
  visibility: visible;
  opacity: 1;
}
</style>
"""


# Not sure what this stuff was....

# page += """
#  <style type="text/css">
#         .tooltip {
#                 border-bottom: 1px dotted #000000; color: #000000; outline: none;
#                 cursor: help; text-decoration: none;
#                 position: relative;
#         }
#         .tooltip span {
#                 margin-left: -999em;
#                 position: absolute;
#         }
#         .tooltip:hover span {
#                 border-radius: 5px 5px; -moz-border-radius: 5px; -webkit-border-radius: 5px;
#                 box-shadow: 5px 5px 5px rgba(0, 0, 0, 0.1); -webkit-box-shadow: 5px 5px rgba(0, 0, 0, 0.1); -moz-box-shadow: 5px 5px rgba(0, 0, 0, 0.1);
#                 font-family: Calibri, Tahoma, Geneva, sans-serif;
#                 position: absolute; left: 1em; top: 2em; z-index: 99;
#                 margin-left: 0; width: 250px;^M
#         }
#         .tooltip:hover img {
#                 border: 0; margin: -10px 0 0 -55px;
#                 float: left; position: absolute;
#         }
#         .tooltip:hover em {
#                 font-family: Candara, Tahoma, Geneva, sans-serif; font-size: 1.2em; font-weight: bold;^M
#                 display: block; padding: 0.2em 0 0.6em 0;^M
#         }
#         .classic { padding: 0.8em 1em; }
#         .custom { padding: 0.5em 0.8em 0.8em 2em; }
#         * html a:hover { background: transparent; }
#         .classic {background: #FFFFAA; border: 1px solid #FFAD33; }
#         .critical { background: #FFCCAA; border: 1px solid #FF3334;     }
#         .help { background: #9FDAEE; border: 1px solid #2BB0D7; }
#         .info { background: #9FDAEE; border: 1px solid #2BB0D7; }
#         .warning { background: #FFFFAA; border: 1px solid #FFAD33; }
#         </style>
#
# """
