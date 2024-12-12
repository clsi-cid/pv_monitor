# ---------------------------------------------------------------------
# Copyright 2024 Canadian Light Source, Inc. All rights reserved
#     - see LICENSE.md for limitations on use.
#
# Description:
#     TODO: <<Insert basic description of the purpose and use.>>
# ---------------------------------------------------------------------
class DESCRIPTION(object):

    BAD_BEACON = """
These are beacons that are going out on the wrong interface.
They need to be tracked down and fixed.  
Beacons with the same IP address and the same (or similar) sequence numbers 
are probably (but not necessarily) from the same IOC. 
"""

    BEACON_ADDR_MISMATCH = """
These are beacons in which the IP address in the beacon does not match
the IP address from which the beacons was received.  
They need to be investigated.
"""

    POLL_CACHE = """
Older versions of PV Monitor polled all detected PVs to determine PV status 
(i.e., lost, duplicate, etc.)  
This proved to be extremely time consuming as the number of PVs increased.  
At last check, it was taking more than 48 hours to poll all PVs.
<p>
To improve this, an aggressive <b><em>Poll Cache</em></b> mechanism 
has been added to PV Monitor. 
A <b><em>poll cache key</em></b> is generated for each PV. 
It consists of the IP address(es) and port(s) of the EPICS app(s) that 
host the PV.  These are determined by the app's EPICS beacon (or beacons 
in the case of multiple apps).
The vast majority of PVs are hosted by a single app 
but there can be multiple apps (e.g., if the PV is accidentally duplicated).  
</p>
<p>
When the PV is to be polled, the <b><em>Poll Cache</em></b> is checked 
and if there is a <b><em>leader</em></b>
PV for the key, the PV is <b><em>not</em></b> polled but rather assigned the 
state of the <b><em>leader</em></b> PV. It <b><em>follows</em></b> the 
<b><em>leader</em></b>. If there is no <b><em>leader</em></b>, the current PV 
is made the <b><em>leader</em></b>for the <b><em>poll cache key</em></b>. 
<b><em>Leader</em></b> PVs are always polled.
</p>
<p>
The <b><em>Poll Cache</em></b> is purged extremely aggressively.  
This is because it can never be known with certainty that a beacon's port and/or
IP address will not change when an app is restarted, or that the PVs hosted 
by the app did not change.  
A PV could move from one app to another.  
Therefore, if there is <b><em>any</em></b> change in a beacon used to 
generate a <b><em>poll cache key</em></b>, the <b><em>Poll Cache</em></b> is
purged of the <b><em>leader</em></b> and all it's <b><em>followers</em></b>.
Finally, since configurations may change when PV Monitor is not running, the
<b><em>Poll Cache</em></b> is rebuilt from scratch when PV Monitor is restarted.
Despite this, the cache hit rate is very high (>95%) and the time to poll
all PVs has been comensurately reduced.
</p>
<p>
Also, PV Monitor no longer polls PVs after they become
lost.  Rather, PV Monitor now relies upon new CA_PROTO_SEARCH responses from 
the EPICS control system to detect the reappearance of lost PVs.  
That is, in the past, lost PVs would eventually be automcatically detected 
when they re-appeared; now they will remain marked as 
lost until a CA_PROTO_SEARCH and response appear on the network.  
This will usually be the case for PVs of interest.  As always, clicking
<b><em>Update Server</em></b> on a PV's Pv Monitor page will force an immediate 
poll regardless of its <b><em>Poll Cache</em></b> status.
</p>
<p>
The purpose of this page is to display the state of the 
<b><em>Poll Cache</em></b>
for testing and debugging purposes. 
</p>
"""
