#!/opt/homebrew/bin/bash

# Deploy to Gateways Script
# This script handles cross-compilation, testing, and deployment of Freenet to gateway servers

set -euo pipefail

# Output colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
BINARIES_DIR="${PROJECT_ROOT}/target/release"
CROSS_BINARIES_DIR="${PROJECT_ROOT}/target/cross-compiled"

# Gateway configurations
declare -A GATEWAYS=(
    ["vega"]="vega.locut.us:x86_64-unknown-linux-gnu:22"
    ["ziggy"]="ziggy.locut.us:aarch64-unknown-linux-gnu:23"
)

# Help function
show_help() {
    echo -e "${BLUE}Deploy to Gateways Script${NC}"
    echo -e "This script cross-compiles, tests, and deploys Freenet to gateway servers.\n"
    echo -e "${YELLOW}Usage:${NC}"
    echo -e "  $0 [options]\n"
    echo -e "${YELLOW}Options:${NC}"
    echo -e "  -h, --help          Show this help message"
    echo -e "  -s, --skip-tests           Skip running tests before deployment"
    echo -e "  -f, --force                Force deployment even if tests fail"
    echo -e "  -g, --gateway NAME         Deploy only to specific gateway (vega or ziggy)"
    echo -e "  -v, --verbose              Show detailed output"
    echo -e "  -p, --ping-only            Deploy only freenet-ping binary"
    echo -e "      --force-old-artifacts  Use GitHub artifacts older than 12 hours\n"
    echo -e "${YELLOW}Target Gateways:${NC}"
    echo -e "  vega (x86_64):   vega.locut.us"
    echo -e "  ziggy (arm64):   ziggy.locut.us (Raspberry Pi)\n"
    echo -e "${YELLOW}GitHub Workflow:${NC}"
    echo -e "  This script downloads binaries from GitHub workflow artifacts."
    echo -e "  If artifacts are older than 12 hours, trigger a new build with:"
    echo -e "    ${GREEN}gh workflow run cross-compile.yml --repo freenet/freenet-core${NC}\n"
}

# Parse command line arguments
SKIP_TESTS=false
FORCE_DEPLOY=false
SPECIFIC_GATEWAY=""
VERBOSE=false
FORCE_OLD_ARTIFACTS=false
PING_ONLY=false

while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_help
            exit 0
            ;;
        -s|--skip-tests)
            SKIP_TESTS=true
            shift
            ;;
        -f|--force)
            FORCE_DEPLOY=true
            shift
            ;;
        -g|--gateway)
            SPECIFIC_GATEWAY="$2"
            shift 2
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -p|--ping-only)
            PING_ONLY=true
            shift
            ;;
        --force-old-artifacts)
            FORCE_OLD_ARTIFACTS=true
            shift
            ;;
        *)
            echo -e "${RED}Unknown option: $1${NC}"
            show_help
            exit 1
            ;;
    esac
done

# Function to log verbose output
log_verbose() {
    if [ "$VERBOSE" = true ]; then
        echo -e "${YELLOW}[VERBOSE]${NC} $1"
    fi
}

# Function to show progress
show_progress() {
    local message=$1
    local status=$2
    
    if [ "$status" = "start" ]; then
        echo -ne "${YELLOW}⏳${NC} $message..."
    elif [ "$status" = "success" ]; then
        echo -e "\r${GREEN}✅${NC} $message"
    elif [ "$status" = "error" ]; then
        echo -e "\r${RED}❌${NC} $message"
    elif [ "$status" = "warning" ]; then
        echo -e "\r${YELLOW}⚠️${NC} $message"
    fi
}

# Function to run tests
run_tests() {
    show_progress "Running tests" "start"
    
    # Capture test output
    local test_output
    test_output=$(cargo test --release 2>&1)
    local test_result=$?
    
    if [ $test_result -ne 0 ]; then
        show_progress "Tests failed" "error"
        if [ "$VERBOSE" = true ] || [ "$FORCE_DEPLOY" = false ]; then
            echo -e "${RED}Test output:${NC}"
            echo "$test_output" | tail -50
        fi
        if [ "$FORCE_DEPLOY" = false ]; then
            echo -e "${RED}Aborting deployment. Use --force to deploy anyway.${NC}"
            exit 1
        else
            echo -e "${YELLOW}WARNING: Tests failed but continuing due to --force flag${NC}"
        fi
    else
        show_progress "Tests passed" "success"
    fi
    
    # Run clippy
    show_progress "Running clippy" "start"
    local clippy_output
    clippy_output=$(cargo clippy -- -D warnings 2>&1)
    local clippy_result=$?
    
    if [ $clippy_result -ne 0 ]; then
        show_progress "Clippy warnings found" "warning"
        if [ "$VERBOSE" = true ]; then
            echo -e "${YELLOW}Clippy output:${NC}"
            echo "$clippy_output" | grep -E "(warning|error)" | head -20
        fi
        if [ "$FORCE_DEPLOY" = false ]; then
            echo -e "${RED}Fix clippy warnings or use --force.${NC}"
            exit 1
        fi
    else
        show_progress "Clippy checks passed" "success"
    fi
}

# Function to check if cross is installed
check_cross_installed() {
    if ! command -v cross &> /dev/null; then
        show_progress "Installing cross" "start"
        cargo install cross
        show_progress "Cross installed" "success"
    fi
}

# Function to compile for target
compile_for_target() {
    local target=$1
    local gateway_name=$2
    
    show_progress "Preparing binary for $gateway_name ($target)" "start"
    
    # Create cross-compiled directory
    mkdir -p "$CROSS_BINARIES_DIR"
    
    local compile_output=""
    local compile_result=0
    
    # Download from GitHub workflow artifacts for both architectures
    show_progress "Downloading binary from GitHub workflow" "start"
    
    # Get the latest successful workflow run (any branch) with timestamp
    local run_info=$(gh run list --repo freenet/freenet-core --workflow cross-compile.yml --status success --limit 1 --json databaseId,createdAt,headBranch --jq '.[0]')
    
    if [ -z "$run_info" ]; then
        compile_output="Failed to find successful workflow run"
        compile_result=1
    else
        local run_id=$(echo "$run_info" | jq -r '.databaseId')
        local created_at=$(echo "$run_info" | jq -r '.createdAt')
        local branch=$(echo "$run_info" | jq -r '.headBranch')
        
        log_verbose "Using workflow run $run_id from branch $branch"
        
        # Check if artifact is older than 12 hours
        local current_time=$(date +%s)
        local artifact_time
        # macOS date compatibility
        if date -r 0 >/dev/null 2>&1; then
            # macOS/BSD date
            artifact_time=$(date -j -f "%Y-%m-%dT%H:%M:%SZ" "${created_at%.*}Z" +%s 2>/dev/null || date -j -f "%Y-%m-%dT%H:%M:%S" "${created_at%.*}" +%s)
        else
            # GNU date (Linux)
            artifact_time=$(date -d "$created_at" +%s)
        fi
        local age_hours=$(( (current_time - artifact_time) / 3600 ))
        
        if [ $age_hours -gt 12 ] && [ "$FORCE_OLD_ARTIFACTS" = false ]; then
            show_progress "GitHub artifacts are $age_hours hours old (> 12 hours)" "error"
            echo -e "${RED}Please trigger a new GitHub workflow run or use --force-old-artifacts${NC}"
            echo -e "${YELLOW}To trigger a new run:${NC} gh workflow run cross-compile.yml --repo freenet/freenet-core"
            compile_result=1
        else
            if [ $age_hours -gt 12 ]; then
                show_progress "Using GitHub artifacts ($age_hours hours old)" "warning"
            fi
            
            # Determine artifact names based on architecture
            local freenet_artifact_name
            local fdev_artifact_name
            local ping_artifact_name
            if [ "$target" = "x86_64-unknown-linux-gnu" ]; then
                freenet_artifact_name="binaries-x86_64-freenet"
                fdev_artifact_name="binaries-x86_64-fdev"
                ping_artifact_name="binaries-x86_64-freenet-ping"
            else
                freenet_artifact_name="binaries-arm64-freenet"
                fdev_artifact_name="binaries-arm64-fdev"
                ping_artifact_name="binaries-arm64-freenet-ping"
            fi
            
            # Download artifacts
            local temp_dir=$(mktemp -d)
            local download_success=true
            
            # Download freenet binary (unless ping-only mode)
            if [ "$PING_ONLY" = false ]; then
                if gh run download "$run_id" --repo freenet/freenet-core --name "$freenet_artifact_name" --dir "$temp_dir" 2>&1; then
                    cp "$temp_dir/freenet" "$CROSS_BINARIES_DIR/freenet-$gateway_name"
                    chmod +x "$CROSS_BINARIES_DIR/freenet-$gateway_name"
                else
                    compile_output="Failed to download freenet binary from workflow run $run_id"
                    download_success=false
                fi
                
                # Download fdev binary
                if gh run download "$run_id" --repo freenet/freenet-core --name "$fdev_artifact_name" --dir "$temp_dir" 2>&1; then
                    cp "$temp_dir/fdev" "$CROSS_BINARIES_DIR/fdev-$gateway_name"
                    chmod +x "$CROSS_BINARIES_DIR/fdev-$gateway_name"
                else
                    compile_output="Failed to download fdev binary from workflow run $run_id"
                    download_success=false
                fi
            fi
            
            # Download freenet-ping binary
            if gh run download "$run_id" --repo freenet/freenet-core --name "$ping_artifact_name" --dir "$temp_dir" 2>&1; then
                cp "$temp_dir/freenet-ping" "$CROSS_BINARIES_DIR/freenet-ping-$gateway_name"
                chmod +x "$CROSS_BINARIES_DIR/freenet-ping-$gateway_name"
            else
                compile_output="Failed to download freenet-ping binary from workflow run $run_id"
                download_success=false
            fi
            
            rm -rf "$temp_dir"
            
            if [ "$download_success" = true ]; then
                compile_result=0
            else
                compile_result=1
            fi
        fi
    fi
    
    if [ $compile_result -ne 0 ]; then
        show_progress "Failed to obtain binary for $gateway_name" "error"
        echo -e "${RED}Error:${NC}"
        echo "$compile_output" | tail -30
        return 1
    else
        show_progress "Binary ready for $gateway_name" "success"
        return 0
    fi
}

# Function to verify gateway deployment
verify_deployment() {
    local gateway_name=$1
    local hostname=$2
    local ssh_opts=$3
    
    # Get versions
    local freenet_version
    local fdev_version
    freenet_version=$(ssh $ssh_opts freenet@$hostname "/usr/local/bin/freenet --version 2>&1" || echo "Unknown")
    fdev_version=$(ssh $ssh_opts freenet@$hostname "/usr/local/bin/fdev --version 2>&1" || echo "Unknown")
    
    # Check for connection success
    local test_output
    test_output=$(ssh $ssh_opts freenet@$hostname "sudo journalctl -u freenet-gateway -n 100 --no-pager" 2>&1 || true)
    
    # Look for successful startup and no critical errors
    if echo "$test_output" | grep -q "Opening network listener" && ! echo "$test_output" | grep -i "panic\|crash\|error.*failed"; then
        echo -e "  ${GREEN}✓${NC} Freenet: $freenet_version"
        echo -e "  ${GREEN}✓${NC} Fdev: $fdev_version"
        echo -e "  ${GREEN}✓${NC} Service: Running"
        return 0
    else
        echo -e "  ${YELLOW}⚠️${NC} Freenet: $freenet_version"
        echo -e "  ${YELLOW}⚠️${NC} Fdev: $fdev_version"
        echo -e "  ${YELLOW}⚠️${NC} Check logs for issues"
        return 1
    fi
}

# Function to deploy to gateway
deploy_to_gateway() {
    local gateway_name=$1
    local gateway_info="${GATEWAYS[$gateway_name]}"
    IFS=':' read -r hostname target port <<< "$gateway_info"
    
    echo -e "\n${BLUE}📦 Deploying to $gateway_name${NC}"
    
    local freenet_binary_path="$CROSS_BINARIES_DIR/freenet-$gateway_name"
    local fdev_binary_path="$CROSS_BINARIES_DIR/fdev-$gateway_name"
    local ping_binary_path="$CROSS_BINARIES_DIR/freenet-ping-$gateway_name"
    
    # Check if required binaries exist
    if [ "$PING_ONLY" = false ]; then
        if [ ! -f "$freenet_binary_path" ]; then
            show_progress "Freenet binary not found for $gateway_name" "error"
            return 1
        fi
        if [ ! -f "$fdev_binary_path" ]; then
            show_progress "Fdev binary not found for $gateway_name" "error"
            return 1
        fi
    fi
    
    if [ ! -f "$ping_binary_path" ]; then
        show_progress "Freenet-ping binary not found for $gateway_name" "error"
        return 1
    fi
    
    # Set SSH options
    local ssh_opts=""
    if [ "$port" != "22" ]; then
        ssh_opts="-p $port"
    fi
    
    # Stop the service (only if deploying freenet)
    if [ "$PING_ONLY" = false ]; then
        show_progress "Stopping service on $gateway_name" "start"
        if ssh $ssh_opts freenet@$hostname "sudo systemctl stop freenet-gateway" 2>/dev/null; then
            show_progress "Service stopped on $gateway_name" "success"
        else
            show_progress "Failed to stop service on $gateway_name" "error"
            return 1
        fi
    fi
    
    # Copy and install freenet binary (unless ping-only mode)
    if [ "$PING_ONLY" = false ]; then
        show_progress "Copying freenet binary to $gateway_name" "start"
        if scp ${ssh_opts//-p/-P} "$freenet_binary_path" "freenet@$hostname:freenet-new" 2>/dev/null; then
            show_progress "Freenet binary copied to $gateway_name" "success"
        else
            show_progress "Failed to copy freenet binary to $gateway_name" "error"
            ssh $ssh_opts freenet@$hostname "sudo systemctl start freenet-gateway" 2>/dev/null
            return 1
        fi
        
        # Install the freenet binary in both locations
        show_progress "Installing freenet binary on $gateway_name" "start"
        if ssh $ssh_opts freenet@$hostname "
            # Install in /usr/local/bin/
            sudo cp freenet-new /usr/local/bin/freenet && 
            sudo chown root:root /usr/local/bin/freenet && 
            sudo chmod 755 /usr/local/bin/freenet &&
            # Install in ~/.cargo/bin/ (create directory if needed)
            mkdir -p ~/.cargo/bin &&
            cp freenet-new ~/.cargo/bin/freenet &&
            chmod 755 ~/.cargo/bin/freenet &&
            rm -f freenet-new
        " 2>/dev/null; then
            show_progress "Freenet binary installed on $gateway_name" "success"
        else
            show_progress "Failed to install freenet binary on $gateway_name" "error"
            ssh $ssh_opts freenet@$hostname "sudo systemctl start freenet-gateway" 2>/dev/null
            return 1
        fi
        
        # Copy and install fdev binary
        show_progress "Copying fdev binary to $gateway_name" "start"
        if scp ${ssh_opts//-p/-P} "$fdev_binary_path" "freenet@$hostname:fdev-new" 2>/dev/null; then
            show_progress "Fdev binary copied to $gateway_name" "success"
        else
            show_progress "Failed to copy fdev binary to $gateway_name" "error"
            ssh $ssh_opts freenet@$hostname "sudo systemctl start freenet-gateway" 2>/dev/null
            return 1
        fi
        
        # Install the fdev binary in both locations
        show_progress "Installing fdev binary on $gateway_name" "start"
        if ssh $ssh_opts freenet@$hostname "
            # Install in /usr/local/bin/
            sudo cp fdev-new /usr/local/bin/fdev && 
            sudo chown root:root /usr/local/bin/fdev && 
            sudo chmod 755 /usr/local/bin/fdev &&
            # Install in ~/.cargo/bin/ (create directory if needed)
            mkdir -p ~/.cargo/bin &&
            cp fdev-new ~/.cargo/bin/fdev &&
            chmod 755 ~/.cargo/bin/fdev &&
            rm -f fdev-new
        " 2>/dev/null; then
            show_progress "Fdev binary installed on $gateway_name" "success"
        else
            show_progress "Failed to install fdev binary on $gateway_name" "error"
            ssh $ssh_opts freenet@$hostname "sudo systemctl start freenet-gateway" 2>/dev/null
            return 1
        fi
    fi
    
    # Copy and install freenet-ping binary
    show_progress "Copying freenet-ping binary to $gateway_name" "start"
    if scp ${ssh_opts//-p/-P} "$ping_binary_path" "freenet@$hostname:freenet-ping-new" 2>/dev/null; then
        show_progress "Freenet-ping binary copied to $gateway_name" "success"
    else
        show_progress "Failed to copy freenet-ping binary to $gateway_name" "error"
        if [ "$PING_ONLY" = false ]; then
            ssh $ssh_opts freenet@$hostname "sudo systemctl start freenet-gateway" 2>/dev/null
        fi
        return 1
    fi
    
    # Install the freenet-ping binary in both locations
    show_progress "Installing freenet-ping binary on $gateway_name" "start"
    if ssh $ssh_opts freenet@$hostname "
        # Install in /usr/local/bin/
        sudo cp freenet-ping-new /usr/local/bin/freenet-ping && 
        sudo chown root:root /usr/local/bin/freenet-ping && 
        sudo chmod 755 /usr/local/bin/freenet-ping &&
        # Install in ~/.cargo/bin/ (create directory if needed)
        mkdir -p ~/.cargo/bin &&
        cp freenet-ping-new ~/.cargo/bin/freenet-ping &&
        chmod 755 ~/.cargo/bin/freenet-ping &&
        rm -f freenet-ping-new
    " 2>/dev/null; then
        show_progress "Freenet-ping binary installed on $gateway_name" "success"
    else
        show_progress "Failed to install freenet-ping binary on $gateway_name" "error"
        if [ "$PING_ONLY" = false ]; then
            ssh $ssh_opts freenet@$hostname "sudo systemctl start freenet-gateway" 2>/dev/null
        fi
        return 1
    fi
    
    # Start the service and verify (only if deploying freenet)
    if [ "$PING_ONLY" = false ]; then
        # Clear journal logs
        ssh $ssh_opts freenet@$hostname "sudo journalctl --vacuum-time=1s -u freenet-gateway" 2>/dev/null || true
        
        # Start the service
        show_progress "Starting service on $gateway_name" "start"
        if ssh $ssh_opts freenet@$hostname "sudo systemctl start freenet-gateway" 2>/dev/null; then
            show_progress "Service started on $gateway_name" "success"
        else
            show_progress "Failed to start service on $gateway_name" "error"
            return 1
        fi
        
        # Wait a moment for service to start
        sleep 3
        
        # Verify deployment
        show_progress "Verifying deployment on $gateway_name" "start"
        if verify_deployment "$gateway_name" "$hostname" "$ssh_opts"; then
            show_progress "Deployment verified on $gateway_name" "success"
        else
            show_progress "Deployment verification failed on $gateway_name" "warning"
            
            # Show error details if verbose
            if [ "$VERBOSE" = true ]; then
                echo -e "${YELLOW}Recent logs from $gateway_name:${NC}"
                ssh $ssh_opts freenet@$hostname "sudo journalctl -u freenet-gateway -n 20 --no-pager | grep -E '(ERROR|WARN|error|failed)'" || true
            fi
            
            echo -e "${YELLOW}To monitor logs:${NC} ssh ${ssh_opts} freenet@$hostname 'sudo journalctl -u freenet-gateway -f'"
            return 1
        fi
    else
        # For ping-only deployment, verify the binary is installed in both locations
        show_progress "Verifying freenet-ping installation on $gateway_name" "start"
        if ssh $ssh_opts freenet@$hostname "
            /usr/local/bin/freenet-ping --version >/dev/null 2>&1 &&
            ~/.cargo/bin/freenet-ping --version >/dev/null 2>&1
        " 2>/dev/null; then
            # Get and display version info
            local ping_version_system
            local ping_version_cargo
            ping_version_system=$(ssh $ssh_opts freenet@$hostname "/usr/local/bin/freenet-ping --version 2>&1" || echo "Unknown")
            ping_version_cargo=$(ssh $ssh_opts freenet@$hostname "~/.cargo/bin/freenet-ping --version 2>&1" || echo "Unknown")
            echo -e "  ${GREEN}✓${NC} System freenet-ping: $ping_version_system"
            echo -e "  ${GREEN}✓${NC} Cargo freenet-ping: $ping_version_cargo"
            show_progress "Freenet-ping installed successfully on $gateway_name" "success"
        else
            show_progress "Freenet-ping installation verification failed on $gateway_name" "warning"
            return 1
        fi
    fi
    
    return 0
}

# Main execution
main() {
    echo -e "${BLUE}🚀 Freenet Gateway Deployment${NC}\n"
    
    # Change to project root
    cd "$PROJECT_ROOT"
    
    # Run tests unless skipped
    if [ "$SKIP_TESTS" = false ]; then
        run_tests
    else
        show_progress "Skipping tests" "warning"
    fi
    
    # Determine which gateways to deploy to
    local gateways_to_deploy=()
    if [ -n "$SPECIFIC_GATEWAY" ]; then
        if [[ -v GATEWAYS[$SPECIFIC_GATEWAY] ]]; then
            gateways_to_deploy=("$SPECIFIC_GATEWAY")
        else
            echo -e "${RED}Unknown gateway: $SPECIFIC_GATEWAY${NC}"
            echo -e "Available gateways: ${!GATEWAYS[@]}"
            exit 1
        fi
    else
        gateways_to_deploy=("${!GATEWAYS[@]}")
    fi
    
    # Compile for each target
    echo -e "\n${BLUE}🔨 Compilation${NC}"
    local compilation_failed=false
    for gateway in "${gateways_to_deploy[@]}"; do
        local gateway_info="${GATEWAYS[$gateway]}"
        IFS=':' read -r hostname target port <<< "$gateway_info"
        
        if ! compile_for_target "$target" "$gateway"; then
            compilation_failed=true
        fi
    done
    
    if [ "$compilation_failed" = true ] && [ "$FORCE_DEPLOY" = false ]; then
        echo -e "\n${RED}Compilation failed. Aborting deployment.${NC}"
        exit 1
    fi
    
    # Deploy to each gateway
    local deployment_results=()
    for gateway in "${gateways_to_deploy[@]}"; do
        if deploy_to_gateway "$gateway"; then
            deployment_results+=("${GREEN}✅ $gateway${NC}")
        else
            deployment_results+=("${RED}❌ $gateway${NC}")
        fi
    done
    
    # Summary
    echo -e "\n${BLUE}📊 Deployment Summary${NC}"
    echo -e "${BLUE}═══════════════════${NC}"
    
    for result in "${deployment_results[@]}"; do
        echo -e "$result"
    done
    
    # Check if all succeeded
    if [[ " ${deployment_results[@]} " =~ "❌" ]]; then
        echo -e "\n${RED}Some deployments failed. Check the errors above.${NC}"
        exit 1
    else
        echo -e "\n${GREEN}✨ All deployments completed successfully!${NC}"
    fi
}

# Run main function
main