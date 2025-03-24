#!/bin/bash

# Output colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Help function
show_help() {
    echo -e "${BLUE}Update Remote Gateways Script${NC}"
    echo -e "This script deploys binary artifacts from the cross-compile.yml GitHub Actions workflow"
    echo -e "to remote Freenet gateway servers.\n"
    echo -e "${YELLOW}Usage:${NC}"
    echo -e "  $0 [options]\n"
    echo -e "${YELLOW}Options:${NC}"
    echo -e "  -h, --help    Show this help message\n"
    echo -e "${YELLOW}Environment Variables:${NC}"
    echo -e "  ARTIFACTS_DIR   Directory containing the binary artifacts (default: current directory)"
    echo -e "  BINARIES_PATH   Temporary path for unpacked binaries (default: $TMPDIR or /tmp)\n"
    echo -e "${YELLOW}Expected Artifacts:${NC}"
    echo -e "  The script expects zip files in the format: binaries-<arch>-<binary>.zip"
    echo -e "  These artifacts are produced by the cross-compile.yml GitHub Actions workflow"
    echo -e "  Example: binaries-x86_64-freenet.zip, binaries-arm64-fdev.zip\n"
    echo -e "${YELLOW}Target Servers:${NC}"
    echo -e "  The script deploys to servers defined in the SERVERS array with their architectures"
    echo -e "  Each server will receive the appropriate architecture binaries"
}

# Check for help flags
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
    show_help
    exit 0
fi

# Configuration
REMOTE_DIR="/usr/local/bin"  # Remote directory where binaries will be copied
BINARIES=("freenet" "fdev")  # Binaries to copy (fixed syntax - no commas)
ARTIFACTS_DIR="${ARTIFACTS_DIR:-$(pwd)}"  # Directory containing the zip artifacts

# Add validation for artifacts directory
if [ ! -d "$ARTIFACTS_DIR" ]; then
    echo -e "${RED}Error: Artifacts directory does not exist: $ARTIFACTS_DIR${NC}"
    exit 1
fi

echo -e "${YELLOW}Using artifacts from: $ARTIFACTS_DIR${NC}"
echo -e "${YELLOW}Available artifact files:${NC}"
ls -la "$ARTIFACTS_DIR"/binaries-*.zip 2>/dev/null || echo -e "${RED}No binary zip files found in $ARTIFACTS_DIR${NC}"

# Server list with architecture mapping
SERVERS=(
    "vega.locut.us:x86_64"
    "technic.locut.us:arm64"
)
BINARIES_PATH="${BINARIES_PATH:-${TMPDIR:-/tmp}}"

# Function to check if artifact exists for given architecture and binary
check_artifact_exists() {
    local arch=$1
    local binary=$2
    local zip_file="${ARTIFACTS_DIR}/binaries-${arch}-${binary}.zip"
    
    if [ -f "${zip_file}" ]; then
        return 0  # True, exists
    else
        return 1  # False, doesn't exist
    fi
}

# Function to unpack binary from zip
unpack_binary() {
    local arch=$1
    local binary=$2
    local zip_file="${ARTIFACTS_DIR}/binaries-${arch}-${binary}.zip"
    
    # Create output directory if it doesn't exist
    mkdir -p "${BINARIES_PATH}"
    
    # Unzip the binary to the output directory
    unzip -o "${zip_file}" -d "${BINARIES_PATH}"
    
    # Return success if the binary now exists
    if [ -f "${BINARIES_PATH}/${binary}" ]; then
        return 0
    else
        return 1
    fi
}

# Function to manage systemd service
manage_service() {
    local server=$1
    local ssh_opts=$2
    local action=$3
    local service="freenet-gateway"
    
    local capitalized_action="$(tr '[:lower:]' '[:upper:]' <<< ${action:0:1})${action:1}"
    echo -e "${YELLOW}${capitalized_action}ing $service service on $server...${NC}"
    
    local result
    result=$(ssh $ssh_opts freenet@$server "sudo systemctl $action $service" 2>&1)
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to $action $service service on $server: $result${NC}"
        return 1
    else
        echo -e "${GREEN}Successfully ${action}ed $service service on $server${NC}"
        return 0
    fi
}

# Function to clear service journal
clear_service_journal() {
    local server=$1
    local ssh_opts=$2
    local service="freenet-gateway"
    
    echo -e "${YELLOW}Clearing journal logs for $service on $server...${NC}"
    
    local result
    result=$(ssh $ssh_opts freenet@$server "sudo journalctl --vacuum-time=1s -u $service" 2>&1)
    
    if [ $? -ne 0 ]; then
        echo -e "${YELLOW}Warning: Failed to clear journal for $service on $server: $result${NC}"
        # Not returning error as this is non-critical
    else
        echo -e "${GREEN}Successfully cleared journal for $service on $server${NC}"
    fi
}

# Function to check service status
check_service_status() {
    local server=$1
    local ssh_opts=$2
    local service="freenet-gateway"
    
    echo -e "${YELLOW}Checking status of $service on $server...${NC}"
    
    local result
    result=$(ssh $ssh_opts freenet@$server "sudo systemctl status $service" 2>&1)
    
    echo -e "${GREEN}Service status on $server:${NC}"
    echo "$result"
    
    if echo "$result" | grep -q "Active: active (running)"; then
        return 0
    else
        echo -e "${RED}WARNING: Service may not be running correctly on $server${NC}"
        return 1
    fi
}

# Function to perform an SCP operation with timeout handling
scp_with_timeout() {
    local src=$1
    local dest=$2
    local port=$3
    local timeout_seconds=60
    
    # Check if timeout command exists
    if command -v timeout &> /dev/null; then
        # Use timeout command if available
        if [ -n "$port" ]; then
            timeout $timeout_seconds scp -P $port "$src" "$dest"
        else
            timeout $timeout_seconds scp "$src" "$dest"
        fi
        return $?
    else
        echo -e "${YELLOW}Warning: timeout command not available, using fallback method${NC}"
        # Fallback to background process with kill
        local pid
        {
            if [ -n "$port" ]; then
                scp -P $port "$src" "$dest"
            else
                scp "$src" "$dest"
            fi
            echo $? > /tmp/scp_exit_status_$$
        } &
        pid=$!
        
        # Wait for process to complete or timeout
        local waited=0
        while [ $waited -lt $timeout_seconds ]; do
            if ! kill -0 $pid 2>/dev/null; then
                # Process completed
                if [ -f /tmp/scp_exit_status_$$ ]; then
                    local exit_status=$(cat /tmp/scp_exit_status_$$)
                    rm -f /tmp/scp_exit_status_$$
                    return $exit_status
                fi
                return 0
            fi
            sleep 1
            waited=$((waited + 1))
        done
        
        # If we get here, the timeout was reached
        echo -e "${YELLOW}SCP operation timed out after $timeout_seconds seconds${NC}"
        kill -9 $pid 2>/dev/null
        rm -f /tmp/scp_exit_status_$$
        return 124  # Standard timeout exit code
    fi
}

# Function to ensure service is completely stopped
ensure_service_stopped() {
    local server=$1
    local ssh_opts=$2
    local service="freenet-gateway"
    local max_attempts=5
    local wait_time=2
    
    echo -e "${YELLOW}Ensuring $service is completely stopped on $server...${NC}"
    
    # First, stop the service
    manage_service "$server" "$ssh_opts" "stop"
    if [ $? -ne 0 ]; then
        echo -e "${RED}Failed to stop the service. Deployment might continue but might fail.${NC}"
        return 1
    fi
    
    # Give the service a moment to fully stop
    sleep 2
    
    # We'll always use the copy method, so we don't need to check if binaries are in use
    echo -e "${GREEN}Service stopped. Will use copy method for installing binaries.${NC}"
    return 0
}

echo -e "${YELLOW}Starting deployment process...${NC}"

# For each server in the list
for server_info in "${SERVERS[@]}"; do
    # Split server_info into server and architecture
    IFS=':' read -r server arch <<< "$server_info"
    
    echo -e "${YELLOW}Deploying to $server (architecture: $arch)...${NC}"
    
    # SSH options
    if [ "$server" = "technic.locut.us" ]; then
        SSH_OPTS="-p 23"
    else
        SSH_OPTS=""
    fi
    
    # Create remote directory if it doesn't exist
    ssh $SSH_OPTS freenet@$server "sudo mkdir -p $REMOTE_DIR"
    
    # Stop the service and make sure binaries are not in use
    ensure_service_stopped "$server" "$SSH_OPTS"
    service_was_stopped=$?
    
    # Flag to track if any binary was updated
    any_binary_updated=0
    
    # Process each binary
    for binary in "${BINARIES[@]}"; do
        if check_artifact_exists "$arch" "$binary"; then
            echo -e "Found artifact for $binary ($arch)"
            
            # Unpack the binary
            if unpack_binary "$arch" "$binary"; then
                echo -e "Unpacked $binary successfully"
                
                # Copy the binary to the server with timeout
                echo -e "${YELLOW}Copying $binary to $server (this may take a moment)...${NC}"
                
                # Use our custom scp_with_timeout function
                if [ "$server" = "technic.locut.us" ]; then
                    scp_with_timeout "${BINARIES_PATH}/${binary}" "freenet@$server:${binary}.new" "23"
                else
                    scp_with_timeout "${BINARIES_PATH}/${binary}" "freenet@$server:${binary}.new" ""
                fi
                
                SCP_RESULT=$?
                if [ $SCP_RESULT -eq 124 ]; then
                    echo -e "${RED}Error: SCP operation timed out after 60 seconds for $binary${NC}"
                    continue
                elif [ $SCP_RESULT -ne 0 ]; then
                    echo -e "${RED}Error copying binary to $server (exit code: $SCP_RESULT)${NC}"
                    continue
                fi
                
                echo -e "${GREEN}File transfer completed successfully${NC}"
                
                # Always use cp method for installation
                echo -e "${YELLOW}Installing $binary to $REMOTE_DIR...${NC}"
                ssh $SSH_OPTS freenet@$server "sudo cp ${binary}.new $REMOTE_DIR/${binary} && sudo chown root:root $REMOTE_DIR/${binary} && sudo chmod 755 $REMOTE_DIR/${binary} && rm -f ${binary}.new"
                
                if [ $? -eq 0 ]; then
                    echo -e "${GREEN}Successfully deployed $binary ($arch) to $server${NC}"
                    any_binary_updated=1
                else
                    echo -e "${RED}Failed to install binary to final location on $server${NC}"
                fi
            else
                echo -e "${RED}Failed to unpack $binary for architecture $arch${NC}"
            fi
        else
            echo -e "${YELLOW}Skipping $binary for architecture $arch (artifact not found)${NC}"
        fi
    done
    
    if [ $service_was_stopped -eq 0 ] && [ $any_binary_updated -eq 1 ]; then
        # Reset any failed state
        ssh $SSH_OPTS freenet@$server "sudo systemctl reset-failed freenet-gateway"
        
        # Clear the service journal
        clear_service_journal "$server" "$SSH_OPTS"
        
        # Restart the service after updating
        manage_service "$server" "$SSH_OPTS" "restart"
        
        # Check service status
        check_service_status "$server" "$SSH_OPTS"
    elif [ $service_was_stopped -eq 0 ] && [ $any_binary_updated -eq 0 ]; then
        echo -e "${YELLOW}No binaries were updated, but restarting service anyway...${NC}"
        manage_service "$server" "$SSH_OPTS" "restart"
        check_service_status "$server" "$SSH_OPTS"
    else
        echo -e "${RED}WARNING: Service was not properly stopped or no binaries were updated.${NC}"
        echo -e "${RED}Manual intervention may be required on $server${NC}"
    fi
    
    echo -e "${GREEN}Deployment completed on $server${NC}"
done

echo -e "${GREEN}Deployment process finished${NC}"