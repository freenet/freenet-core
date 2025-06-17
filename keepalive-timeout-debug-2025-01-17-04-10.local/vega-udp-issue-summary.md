# Vega UDP Packet Drop Issue Summary

## Problem Statement
The vega EC2 instance (Ubuntu 24.04 LTS) has a critical issue where UDP packets are being silently dropped between the application layer and the network interface, causing Freenet gateway connection timeouts after 30 seconds.

## Key Evidence

### 1. Application Layer Claims Success
- `socket.send_to()` returns success with correct byte count
- Application logs show "successful" sends every 10 seconds
- No errors reported at the socket API level

### 2. Kernel Statistics Show Packets "Sent"
- `/proc/net/snmp` shows `OutDatagrams` counter incrementing
- This means packets reach the kernel's UDP layer
- No errors in kernel statistics (no OutErrors, SndbufErrors, etc.)

### 3. Network Layer Shows ZERO Packets
- `tcpdump -i any` shows NO outgoing UDP packets
- `tcpdump -i enX0` (specific interface) shows NO outgoing UDP packets
- Only incoming packets are visible in tcpdump
- This affects ALL UDP traffic from the instance

### 4. Test Results
- Simple C program using raw UDP sockets: same issue (send_to success, but no packets on wire)
- Bound socket test (binding to 0.0.0.0:31338): same issue
- TCP traffic works fine (SSH, etc.)
- Issue is specific to UDP outbound traffic

## System Configuration
- **OS**: Ubuntu 24.04 LTS (noble)
- **Kernel**: 6.8.0-1012-aws #13-Ubuntu SMP
- **Network Interface**: enX0 (driver: vif, virtual interface)
- **EC2 Instance**: vega.locut.us (100.27.151.80)
- **AWS Region**: N. Virginia
- **Security Groups**: Correctly configured (verified)
- **iptables**: No blocking rules (OUTPUT chain ACCEPT all)

## Unusual Findings
1. **Interface name "enX0"** - not the typical eth0/ens5
2. **Driver "vif"** - virtual interface driver
3. **Kernel messages show**: "vif vif-0 enX0: entered promiscuous mode" (from tcpdump)
4. **OutDatagrams increments but packets never reach tcpdump** - this is extremely unusual

## Impact
- Freenet gateway on vega cannot send keep-alive packets
- All connections timeout after 30 seconds
- River app and other services relying on vega gateway are affected

## What We've Ruled Out
- ✗ Not a Freenet bug (code works on other systems)
- ✗ Not AWS security groups or Network ACLs
- ✗ Not iptables firewall rules
- ✗ Not a rate limiting issue
- ✗ Not a socket option problem

## Hypothesis
There appears to be a kernel or virtualization layer issue where UDP packets are being dropped after the kernel's UDP layer but before they reach the network interface driver. This could be:
1. A bug in the vif driver
2. An EC2 hypervisor issue
3. A kernel bug specific to this version
4. Some virtualization layer packet filtering we can't see

## Questions for Research
1. Are there known issues with UDP on Ubuntu 24.04 with kernel 6.8.0-1012-aws?
2. What is the "vif" driver and are there known UDP issues with it?
3. Why would packets increment kernel counters but not be visible to tcpdump?
4. Could this be related to EC2 network virtualization (Nitro system)?
5. Are there any sysctls or kernel parameters that could cause this behavior?