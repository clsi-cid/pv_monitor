# PV Monitor

PV Monitor is a tool that monitors the state of PVs, IOC applications, and IOC hosts, using approaches such as monitoring channel access network traffic.

## How to start PV Monitor

### Setup

-   Two configuration files are needed to set up the system for your site's configuration. For this purpose, we have provided configuration file templates, named `configTemplate.yml` and `configTemplate_gw.yml`, check the [PVMonitor_Configuration documentation](documentation/PVMonitor_Configuration.md) for configuraton file details.

-   A large portion of the PV Monitor data is storage in MariaDB database (check [PV Monitor Architecture documentation](documentation/PVMonitor_Mechanism_and_Architecture.md) for more). A MariaDB database needs to be set up before PV Monitor the first time.

-   Some database information should be added to the configuration file but a file named `password.py`, which contains password of the database, should also be created.\
    \
    `PASSWORD=""`

-   You need a number of servers are needed to run PV Monitor. These include the one on which the main `PV Monitor` application runs, and another one, which has access to all the EPICS network interfaces of interest:

    -   a CAINFO server (for which the code is in the Tool_EpicsServer git repository).

    -   a gateway server which we call `GatewaySearcher` with all EPICS network interfaces (subnets) that allow PV Monitor see all channel access traffic

-   A file called `gateway_search_worker.py` also needs to be imported from the `pv_monitor_utils` repo to the `PVMonitor_gateway` directory. The `gateway_search_worker.py` file is stored outside PV Monitor repository, as this piece of considered potentially dangerous because it contains customized UDP package (`CA_PROTO_SEARCH`) which will be broadcast out by `GatewaySearcher`. This could cause disruption to the whole system if any errors are inadvertently introduced to the code.

### Environment

PV monitor is preferably run using Python 3.9 or higher. There are a number package dependencies. The conda environment YAML, [environment.yml](environment.yml) is provide in the main directory.

### Run PV Monitor Gateway server

1.  Go to `PVMonitor_gateway` directory
2.  Run `<python3.9 or higher> gateway.py -m <prod/test> -c <PVM client server> --config <gateway config ``file> -l INFO`

### Run PV Monitor client server

1.  Go to PVM client server
2.  In the main directory, run `<python3.9 or higher> combined.py -m <prod/test> -g <PVM gateway server> --config <client config file> -l INFO`

### Run EpicsServer

EpicsServer is not the same repo as PV Monitor and it is a separate repo, but here still is provided instructions on how to run EpicsServer

1.  Go to PVM Gateway server
2.  Run `<python3> server.py`
