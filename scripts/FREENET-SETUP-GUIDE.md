# Freenet Node and Gateway Setup Guide

## Introduction

This guide provides step-by-step instructions for setting up a **Freenet Gateway** or **Node** using the `freenet-setup.sh` script. The script automates the configuration process, generates required keys, and prepares the system for deployment.

## Requirements

### System Requirements

- A Unix-based operating system (e.g., Ubuntu, Debian, CentOS)
- Git
- OpenSSL
- curl
- SSH access (if deploying a node)

### Network Requirements

- Open ports:
    - **31337** for network communication
    - **3001** (Gateway WS API)
    - **3002** (Node WS API)

### Pre-Installed Tools

Make sure the following tools are installed:

```bash
sudo apt update
sudo apt install -y git openssl curl
```

## Script Overview

The `freenet-setup.sh` script supports two modes:

1. **Gateway Mode** (`gateway`) - Sets up a Freenet Gateway.
2. **Node Mode** (`node`) - Sets up a Freenet Node connected to an existing Gateway.

## Usage

### Clone the Repository

```bash
git clone https://github.com/freenet/freenet-core.git
cd freenet-core
chmod +x freenet-setup.sh
```

### Script Syntax

```bash
./freenet-setup.sh <name> <type> <gw-host>
```

- **<name>**: Unique name for your node or gateway.
- **<type>**: `gateway` or `node`
- **<gw-host>**: Hostname or IP:Port of the Gateway (required only for `node`).

### Examples

#### Gateway Setup Example

```bash
./freenet-setup.sh freenet-gw gateway
```

- Sets up a Gateway node named `freenet-gw`.

#### Node Setup Example

```bash
./freenet-setup.sh freenet-n1 node 192.168.1.10:31337
```

- Sets up a Node named `freenet-n1` connected to a Gateway at `192.168.1.10:31337`.

## Configuration Details

### Generated Directories and Files

After running the setup, the following directories and files will be created:

- **.cache/freenet**: Stores keys, logs, configuration files, and runtime data.
- **freenet/**: Contains the repository, scripts, and other operational files.

#### Specific Paths:

- **Keys Directory**: `~/.cache/freenet/keys`
- **Logs Directory**: `~/.cache/freenet/logs`
- **PIDs Directory**: `~/.cache/freenet/pids`
- **Node Directory**: `~/.cache/freenet/<name>`
- **Gateways Configuration**: `~/.cache/freenet/gateways.toml`
- **Startup Scripts**: `~/freenet/start-<name>.sh`

### Gateways Configuration (gateways.toml)

For **Node Mode**, the script generates a `gateways.toml` file based on whether `gw-host` is an IP or a hostname:

- **IP:PORT**:

```toml
[[gateways]]
address = { host_address = "192.168.1.10:31337" }
public_key = "~/freenet/freenet-gw_public_key.pem"
```

- **Hostname**:

```toml
[[gateways]]
address = { hostname = "gateway.local" }
public_key = "~/freenet/freenet-gw_public_key.pem"
```

## Start the Node or Gateway

After running the setup, a startup script will be generated:

```bash
~/freenet/start-<name>.sh
```

Make it executable and start the service:

```bash
chmod +x ~/freenet/start-<name>.sh
~/freenet/start-<name>.sh
```

## Verify the Service

- **Logs**: Check the logs located in `~/.cache/freenet/logs/<name>.log`
- **Processes**: Verify the service is running:

```bash
ps -aux | grep freenet
```

- **Check Ports**:

```bash
sudo netstat -tulnp | grep 31337
```

## Stop the Service

```bash
kill $(cat ~/.cache/freenet/pids/<name>.pid)
```

## Troubleshooting

1. **Permission Errors:** Ensure the script has executable permissions.

```bash
chmod +x freenet-setup.sh
```

2. **SSH Issues (for Node Mode):** Verify access to the Gateway.
3. **Port Conflicts:** Ensure required ports are available.