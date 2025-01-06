Here’s the full README in Markdown format for you to copy and paste:

# **Freenet Gateway Setup Guide with systemd**

## **1. Introduction**
This guide provides detailed instructions for setting up and managing a **Freenet Gateway** service using `systemd` with the `init-gateway.sh` script.

---

## **2. Prerequisites**

Ensure you have the following installed:

- A Unix-based OS (e.g., **Ubuntu**, **Debian**, **CentOS**)
- `systemd`
- `openssl`
- `curl`
- `git`
- The latest version of `freenet` utility available in `/usr/local/bin/` via a symbolic link to the target directory where it is installed.

---

## **3. File Structure**

- **freenet-gateway.service:** systemd unit file for managing the Freenet Gateway.
- **init-gateway.sh:** Initialization script to start the Freenet Gateway service.

---

## **4. Setup Steps**

### **4.1. Copy Required Files**

1. **Copy `freenet-gateway.service` to systemd directory:**
      ```bash
       sudo cp freenet-gateway.service /etc/systemd/system/
      ```

2.	Copy init-gateway.sh to /usr/local/bin/:

      ```bash
      sudo cp init-gateway.sh /usr/local/bin/
      sudo chmod +x /usr/local/bin/init-gateway.sh
      ```

3.	Set Ownership (if required):
    
      ```bash
      sudo chown root:root /etc/systemd/system/freenet-gateway.service
      sudo chown root:root /usr/local/bin/init-gateway.sh
      ```
---

### **4.2. Update Environment Variables (if needed)**

Edit the service file if paths or parameters differ:

```bash
  sudo nano /etc/systemd/system/freenet-gateway.service
```

Update values such as:
- TRANSPORT_KEYPAIR
- PUBLIC_NETWORK_ADDRESS
- PUBLIC_NETWORK_PORT

Save and exit.

## **5. Enable and Start the Service**
### **1. Reload systemd to recognize the new service:**

  ```bash
    sudo systemctl daemon-reload
  ```

### **2. Enable the service to start on boot:** 

  ```bash
    sudo systemctl enable freenet-gateway
  ```


### **3. Start the service:**

  ```bash
    sudo systemctl start freenet-gateway
  ```

### **4. Verify the service status:**

  ```bash
    sudo systemctl status freenet-gateway
  ```

---

## **6. Managing the Service**

### **1. View logs:**

  ```bash
    sudo journalctl -u freenet-gateway -f
  ```

### **2. Restart the service:**

  ```bash
    sudo systemctl restart freenet-gateway
  ```

### **3. Stop the service:**
    
  ```bash
    sudo systemctl stop freenet-gateway
  ```

---

## **7. Uninstalling the Service**

To remove the service:

  ```bash
    sudo systemctl stop freenet-gateway
    sudo systemctl disable freenet-gateway
    sudo rm /etc/systemd/system/freenet-gateway.service
    sudo rm /usr/local/bin/init-gateway.sh
    sudo systemctl daemon-reload
  ```