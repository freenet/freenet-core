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

- Open ports (gateway only):
    - **31337** default network api port (or alternate port if specified)

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

### Create a freenet directory

```bash
  mkdir freenet
```

### Clone the Repository

```bash
  cd freenet
  git clone https://github.com/freenet/freenet-core.git
  cd freenet-core/scripts
  chmod +x freenet-node-setup.sh
```

### Script Syntax

```bash
  ./freenet-node-setup.sh <name> [ws-api-port]
```

- **<name>**: Unique name for your node or gateway.
- **[ws-api-port]**: Optional WebSocket API port (default: 50509).

### Examples

#### Node Setup Example

```bash
  ./freenet-node-setup.sh freenet-n1
```

- Sets up a Node named `freenet-n1` connected to a freenet gateways. The gateways config is downloaded from freenet.org by default.

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
- **Node Configuration**: `~/.cache/freenet/node.toml`
- **Startup Scripts**: `~/freenet/start-<name>.sh`

## Start the Node

After running the setup, a startup script will be generated:

```bash
  ~/freenet/init-<name>.sh
```

Make it executable and start the service:

```bash
  chmod +x ~/freenet/init-<name>.sh
  ~/freenet/init-<name>.sh
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
  chmod +x freenet-node-setup.sh
```

2. **SSH Issues (for Node Mode):** Verify access to the Gateway.
3. **Port Conflicts:** Ensure required ports are available.
