# Demo video script (≤5 min), to record on the Raspberry Pi
1. 0:00 Show `pipeline status` online, and cloud results arriving (`pipeline results`).
2. 1:00 **Network cut**: `sudo ip link set wlan0 down` (or pull Ethernet). Run `watch -n1 pipeline status` and show the switch to offline within 10 s, with offline results being produced. Restore the network, show it back online, and show the backlog draining to 0.
3. 2:30 **Process kill**: `sudo pkill -9 -f "pipeline run"`. systemd restarts it and `status` keeps counting.
4. 3:15 **Power cut**: pull the power plug, then boot. Show `pipeline export --from device | tail` with the open-ended loss line.
5. 4:15 Run `check_ground_truth` on both the device and cloud exports.
