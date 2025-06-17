# Vega Gateway Investigation Results

## Critical Finding: Application Says It's Sending, But Packets Never Leave!

### Evidence:
1. **Application logs show keep-alives being sent**:
   - "Sending keep-alive NoOp packet"
   - "Keep-alive NoOp packet sent successfully"
   - Sending every 10 seconds as expected

2. **tcpdump shows NO outbound packets**:
   - Captured 20+ inbound packets from our machine
   - ZERO outbound packets to our machine
   - Command: `tcpdump -i any -n 'udp port 31337 and src host 100.27.151.80 and dst host 136.62.52.28'`

3. **Continuous decryption errors**:
   - Vega logs show constant "decryption error" for incoming packets
   - This suggests wrong keys or protocol mismatch

## System Configuration:
- AWS security groups: ✅ Correct (allows UDP 31337)
- Network ACLs: ✅ Correct (allows all)
- iptables: ✅ No rules blocking
- Socket listening: ✅ UDP 31337 bound correctly

## Root Cause Analysis:
The packets are being dropped somewhere between:
1. Application calling send() -> ✅ (logs show success)
2. Kernel network stack -> ❌ (tcpdump sees nothing)

## Possible Causes:
1. **Wrong destination address** - Application might be sending to wrong IP/port
2. **Socket configuration issue** - Socket might not be properly configured
3. **Memory/buffer issue** - Packets might be malformed
4. **Gateway-specific bug** - Gateway mode might have different send logic

## Next Steps:
1. Check what address the application is actually sending to
2. Trace the send path in the gateway code
3. Check if gateway connections use different socket configuration