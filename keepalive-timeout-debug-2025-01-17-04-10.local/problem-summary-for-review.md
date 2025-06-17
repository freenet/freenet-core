# UDP Packet Drop Problem on Vega EC2 Instance - Complete Summary

## The Problem
A vanilla Ubuntu 24.04 EC2 instance (vega.locut.us) cannot send UDP packets. The packets appear to be sent successfully at the application layer but never actually leave the network interface. This causes all Freenet gateway connections to timeout after 30 seconds.

## What We Observed

### 1. Application Layer Behavior
- `socket.send_to()` returns SUCCESS with correct byte count
- No errors reported (errno is 0)
- Application logs show "packet sent" messages
- This happens with both our Rust application AND a minimal C test program

### 2. Kernel Statistics Behavior
- `/proc/net/snmp` shows `OutDatagrams` counter INCREMENTS
- This proves packets reach the kernel's UDP layer
- No error counters increment (OutErrors, SndbufErrors all remain 0)
- The kernel thinks it successfully processed the packets

### 3. Network Interface Behavior
- `tcpdump -i any` shows ZERO outgoing UDP packets
- `tcpdump -i enX0` (specific interface) shows ZERO outgoing UDP packets
- We can see INCOMING UDP packets just fine
- TCP traffic works perfectly (SSH, HTTPS, etc.)

### 4. Test Results
```c
// This simple test program exhibits the same issue:
int sock = socket(AF_INET, SOCK_DGRAM, 0);
sendto(sock, "test", 4, 0, &dest_addr, sizeof(dest_addr));
// Returns: 4 (success)
// tcpdump shows: NOTHING
// /proc/net/snmp OutDatagrams: INCREMENTS
```

## What We've Checked and Ruled Out

### Network Configuration
- ✓ AWS Security Groups: Correctly configured, allow all outbound
- ✓ Network ACLs: Default (allow all)
- ✓ iptables: OUTPUT chain is ACCEPT all
- ✓ No traffic control (tc) rules
- ✓ No network namespaces
- ✓ Route table is normal
- ✓ MTU is standard (9001 for AWS)

### System Configuration
- ✓ No unusual sysctl settings
- ✓ Socket buffer sizes are normal (212992)
- ✓ No AppArmor/SELinux denials
- ✓ dmesg shows no errors
- ✓ Instance was working fine before, no configuration changes

### Application Issues
- ✓ Not a Freenet bug (works on other systems)
- ✓ Not a Rust/tokio issue (C program has same problem)
- ✓ Not a permissions issue (happens with sudo too)
- ✓ Not a binding issue (happens with both bound and unbound sockets)

## System Details
- **Instance**: EC2 in N. Virginia
- **OS**: Ubuntu 24.04 LTS
- **Kernel**: 6.8.0-1012-aws #13-Ubuntu SMP
- **Network Interface**: enX0 (unusual name)
- **Driver**: vif (virtual interface)
- **Instance has been up**: Multiple days

## The Mystery
1. Why do packets increment kernel counters but never reach tcpdump?
2. Why does this only affect UDP (not TCP)?
3. Why is the interface named "enX0" instead of eth0/ens5?
4. Is the "vif" driver significant?

## Critical Observations
- This is a **vanilla Ubuntu EC2 instance** - no custom configuration
- The instance was working before (though we don't know exactly when it stopped)
- Multiple services are affected (Freenet gateway, River app)
- Even a minimal 5-line C program exhibits the issue

## Question for Review
Given this is supposedly a standard EC2 instance with no special configuration, what obvious things might we be missing? The symptoms are very specific: UDP packets counted by kernel but invisible to tcpdump, while TCP works fine.