# PV Monitor Configuration

PV Monitor stores its configuration in a YAML file, and there are 2 separate configuration files are required, one is for `PV Monitor`, one is for `GatewaySearcher` and the configuration files are called in the command lines that start `PV Monitor` and `GatewaySearcher`:

- `<python3.9 or higher> gateway.py -m <prod/test> -c <PVM client server> --config <gateway config file> -l INFO`
- `<python3.9 or higher> combined.py -m <prod/test> -g <PVM gateway server> --config <client config file> -l INFO`

## Configuration file template for PV Monitor

```
### Template Config file ###

###Network Config###
HOST_NAME:
  PVMonitorHost:
  GatewaySearcherHost:
  CAINFOHost:

GatewaySearcher_ADDRESS:
  IP_Addr: []
  BCAST_Addr: []

DB:
  DB_Server:
  DB_User:
  DB_Name:

PORT:
  HTTP_PORT: 5073
  TO_CLIENT_PORT: 5072
  TO_GATEWAY_PORT: 5080

HOSTNAME_IPS:
  VM_ARCHIVER_02: []

FILE_SAVE_INTERVAL:
  SECONDS: 0
  MINUTES: 0
  HOURS: 4
  DAYS: 0

# Linux monitors older than this will not be polled
MAX_LINUXMON_AGE:
  SECONDS: 0
  MINUTES: 0
  HOURS: 0
  DAYS: 365

# PVs older than this will be purged
PV_PURGE_AGE:
  SECONDS: 0
  MINUTES: 0
  HOURS: 0
  DAYS: 365

# Hostname cache expiration time
MAX_HOST_AGE:
  SECONDS: 0
  MINUTES: 0
  HOURS: 1
  DAYS: 0
```

| Config                  | Description                                                                                                                                                                                                                                                   |
| ----------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| HOST_NAME               |                                                                                                                                                                                                                                                               |
| `PVMonitorHost`         | The host name of PV Monitor server                                                                                                                                                                                                                            |
| `GatewaySearcherHost`   | The host name of the PV Monitor gateway server (GatewaySearcher server)                                                                                                                                                                                       |
| `CAINFOHost`            | The host name of the server which host the EpicsServer (normally on the same server as the GatewaySearcherHost)                                                                                                                                               |
| GatewaySearcher_ADDRESS |                                                                                                                                                                                                                                                               |
| `IP_Addr`               | A list of IP addresses (subnets) of the PV Monitor gateway machine                                                                                                                                                                                            |
| `BCAST_Addr`            | A list of broadcast addresses of the PV Monitor gateway machine                                                                                                                                                                                               |
| DB                      |                                                                                                                                                                                                                                                               |
| `DB_Server`             | The Server hostname which hosts the PV Monitor database                                                                                                                                                                                                       |
| `DB_User`               | The PV Monitor MariaDB database username                                                                                                                                                                                                                      |
| `DB_Name`               | The PV Monitor MariaDB database name                                                                                                                                                                                                                          |
| PORT                    |                                                                                                                                                                                                                                                               |
| `HTTP_PORT`             | The HTTP port of the PV Monitor server listens web requests from client                                                                                                                                                                                       |
| `TO_CLIENT_PORT`        | The port that GatewaySearcher used to send data to the PV Monitor main application to process                                                                                                                                                                 |
| `TO_GATEWAY_PORT`       | The port that GatewaySearcher used to broadcast UDP package and listen the chennal access traffic                                                                                                                                                             |
| HOSTNAME_IPS            |                                                                                                                                                                                                                                                               |
| `VM_ARCHIVER_02`        | VM_ARCHIVER_02 with mutiple network interfaces is a archiver machine used by CLS, can leave empty. A list of IP addresses of the archiver machines, PV Monitor GatewaySearcher will ignore the network traffic (mostly CA_PROTO_SEARCH) coming from these IPs |
| Some time intervals     |                                                                                                                                                                                                                                                               |
| `FILE_SAVE_INTERVAL`    | The time interval for PV Monitor to save the PV data file which contains all PVs                                                                                                                                                                              |
| `MAX_LINUXMON_AGE`      | The max age of the LinuxMonitor, Linux monitors older than this will not be polled                                                                                                                                                                            |
| `PV_PURGE_AGE`          | PVs older than this will be purged                                                                                                                                                                                                                            |
| `MAX_HOST_AGE`          | Hostname cache expiration time                                                                                                                                                                                                                                |

## Configuration file template for PV Monitor gateway

```
###Network Config###
HOST_NAME:
  GatewaySearcherHost:

GatewaySearcher_ADDRESS:
  IP_Addr:
  BCAST_Addr:

PORT:
  TO_CLIENT_PORT:
  TO_GATEWAY_PORT:

HOSTNAME_IPS:
```

| Config                  | Description                                                                                       |
| ----------------------- | ------------------------------------------------------------------------------------------------- |
| HOST_NAME               |                                                                                                   |
| `GatewaySearcherHost`   | The hostname of the server which host the PV Monitor gateway (GatewaySearcherHost)                |
| GatewaySearcher_ADDRESS |                                                                                                   |
| `IP_Addr`               | A list of IP addresses (subnets) of the PV Monitor gateway machine                                |
| `BCAST_Addr`            | A list of broadcast addresses of the PV Monitor gateway machine                                   |
| PORT                    |                                                                                                   |
| `TO_CLIENT_PORT`        | The port that GatewaySearcher used to send data to the PV Monitor main application to process     |
| `TO_GATEWAY_PORT`       | The port that GatewaySearcher used to broadcast UDP package and listen the chennal access traffic |
