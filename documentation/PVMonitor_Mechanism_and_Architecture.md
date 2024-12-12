# PV Monitor Mechanism and Architecture

This document is about the overall mechanism and architecture of PV Monitor and is meant to help people who would like to understand how PV Monitor works internally. As well as help the developers and maintainers of PV Monitor.

Abstractly speaking, PV Monitor is a software that sniffs the network traffic of the CLS and then analyzes, displays, and saves the data. Therefore, PV Monitor is a combination of two things: the sniffer or the `Gateway`, and the `PV_Monitor`.

## A big picture of PV Monitor internal mechanism

### Gateway

Gateway listens the following network traffic in file `gateway.py`:

**PVs request** - PV request is the EPICS channel access [CA_PROTO_SEARCH](https://epics.anl.gov/base/R3-16/1-docs/CAproto/index.html#ca-proto-search) protocol.

**PVs request response** - PV request response is part of the EPICS channel access [CA_PROTO_SEARCH](https://epics.anl.gov/base/R3-16/1-docs/CAproto/index.html#ca-proto-search:~:text=the%20requested%20channel.-,8.2.2.%20Response,-Table%209.%20Header) protocol (the response section)

**IOC app beacons** \- detail for beacons can be found [here](https://epics.anl.gov/docs/APS2014/05-CA-Concepts.pdf).

**IOC app heartbeats** \- [heartbeats](https://github.lightsource.ca/cid/TOOL_heartbeat) are customize process within each IOC app that broadcast IOC app info to the network.

Gateway also actively searches for PV information by sending CA_PROTO_SEARCH to update PV information in PV Monitor. This is done in the file `gateway_search_worker.py`.

### PV Monitor

PV Monitor creates the front-end web page powered by [bottle](https://bottlepy.org/docs/dev/) and all web pages are constructed in the file `combined.py`. In most of the cases `combined.py` calls the file `dataman.py`, which manages data among different manager classes, and calls the middle-ware-like file `gateway_client.py` which helps the communication between the PV Monitor and the Gateway.
<br/>The above is a very general view of PV Monitor, the rest of this document will explain more about PV Monitor from its in detailed network data handling architecture and logic, to the data process mechanism and architecture.

## Network data handling

### Network level

PV Monitor retrieves network data by passively listening for UDP packages in the network and also actively makes UDP requests to search for data.  
The following figure describes how PVM passively listens to network traffic.

![Passively listen](/documentation/images/passive_listen.png)

When clients request for PV, a [CA_PROTO_SEARCH](https://epics.anl.gov/base/R3-16/1-docs/CAproto/index.html#ca-proto-search) protocol package is sent to the network (UDP port 5064), whichever server responds to that search also sends UDP package to the network, the IOC apps broadcast Beacon (UDP port 5065) and heartbeat data (UDP port 5068) to the network. PV Monitor is listening to all these traffic on the network.

PV Monitor also actively sends out data when it wants to get updated PV information:

![Actively Search](/documentation/images/active_search.png)

For example, each time a PVs table gets loaded, PV Monitor sends CA_PROTO_SEARCH request for PVs on that table, after PV Monitor receives the responses from the servers, the PVs update is done.

#### EPICS Server

PV CAINFO data in PV Monitor is currently coming from an EpicsServer process which runs parallel to PV Monitor, the following diagram illustrate this.

![EPICS Server](/documentation/images/epics_server.png)

Whenever PV Monitor requests for CAINFO of a certain PV, PV Monitor actually makes a request like this:

```
"http://%s:%d/cainfo/" % (CAINFO_HOST, PORT.CAINFO_SERVER) + "pv_name"
```

Then the PV Monitor get cainfo data from the HTTP response.

### PV Monitor object level

This section will talk about in detailed how PV Monitor receives, sends, and process the network data in the level of PV Monitor code

![Network data flow in PV Monitor](/documentation/images/network_data.png)

The above figure illustrates the network data flow between PV Monitor, PVM Gateway, and the CLS network. The `call` label of the connector means a function in one file calls a function in another file, no sending or receiving network data involved.

The blue connectors show the flow of PV search request message (NOTE: this search request is not CA_PROTO_SEARCH, it is just request to search for search PVs). When a PVs table is loaded, the `combined.py` file calls `\_dataman.\_searcher.search`, the `\_dataman` is an instance of the `DataManager` object in file `dataman.py`, the `searcher` is an instance of `NewSearcher` object in `gateway_client.py` , the `search` function puts a certain PV into the search queue. Then the search request is sent to the PVM Gateway IP address with a dedicated client to gateway port. Then `gateway.py` receives the search request from the network, call `gateway_search_worker.py` to do a CA_PROTO_SEARCH for the PV, the updated PV information response will be sent by the host of that PV.

The green connectors show how PV Monitor/ PVM Gateway receive PV search, PV search response, heartbeat, and beacon data from the network. Then the gateway.py sends this data to the gateway IP, the Receiver Object defined in gateway_client receives this data for further processing.

## PV Monitor data storage

PV Monitor processes a large amount of data and it is important to store and cache data. The following diagram illustrates how PV Monitor stores data.

![Data storage](/documentation/images/data_storage.png)

In general, PV Monitor stores data in MariaDB database, JSON files, and CSV files. Since there are a lot of PV information (as June 4 2024, there are around 0.75 million PVs in the production PV Monitor, including online, lost, and never seen PVs), it will take a very long time for to find and load all PVs for each time restarting PV Monitor. It would be very useful to have the PV data stored when PV Monitor restarts and also useful during the run time. PV Monitor stores a snapshot of the all the PVs information for each 4 hours in a JSON file. The JSON file contains both PVs data and LinuxMonitor PVs data.

PV Monitor also stores statistic data, like count of PVs requests made by clients, PVs requests made by PVM Gateway, number of PVs, number of lost PVs, lastest request PVs, etc. This information is stored in a new CSV file each day.

For data that requires more precise use, like app information, IOC information, beacon information, etc., are stored in the MariaDB. A separate database is used for CID project deployment.
