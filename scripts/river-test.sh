#!/bin/bash

# ==========================================
# River Two-User Chat Test Script
# ==========================================

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
USER1_CONFIG_DIR="$HOME/.river-user1"
USER2_CONFIG_DIR="$HOME/.river-user2"
ROOM_NAME="Test Room"
USER1_NICKNAME="Alice"
USER2_NICKNAME="Bob"
FREENET_WS_URL_USER1="ws://127.0.0.1:3001/v1/contract/command?encodingProtocol=native"  # User 1 connects to first node
FREENET_WS_URL_USER2="ws://127.0.0.1:3002/v1/contract/command?encodingProtocol=native"  # User 2 connects to second node

# Temporary files to store keys and codes
ROOM_KEY_FILE="/tmp/river_room_key"
INVITATION_CODE_FILE="/tmp/river_invitation_code"

# Functions
log_step() {
    echo -e "${BLUE}→ $1${NC}"
}

log_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

log_error() {
    echo -e "${RED}✗ $1${NC}"
}

log_info() {
    echo -e "${YELLOW}ℹ $1${NC}"
}

cleanup() {
    log_step "Cleaning up temporary files..."
    rm -f "$ROOM_KEY_FILE" "$INVITATION_CODE_FILE"
}

# Trap to cleanup on exit
trap cleanup EXIT

echo -e "${GREEN}==========================================
River Two-User Chat Test
==========================================${NC}"

# Step 1: Check if Freenet is running
log_step "Checking if Freenet nodes are running..."
if ! pgrep -f "freenet network" > /dev/null; then
    log_error "Freenet network is not running!"
    log_info "Please start the network first with: make -f scripts/local-network.mk start"
    exit 1
fi
log_success "Freenet network is running"

# Step 2: Setup config directories
log_step "Setting up River config directories..."
mkdir -p "$USER1_CONFIG_DIR" "$USER2_CONFIG_DIR"
log_success "Config directories created"

# Step 3: User 1 creates a room
log_step "User 1 (Alice): Creating room '$ROOM_NAME'..."
output=$(riverctl --node-url "$FREENET_WS_URL_USER1" room create --name "$ROOM_NAME" --nickname "$USER1_NICKNAME" 2>&1)
if [[ $? -ne 0 ]]; then
    log_error "Failed to create room"
    echo "$output"
    exit 1
fi

log_info "Room creation output:"
echo "$output"

# Extract room owner key - look for the key pattern in the output
room_key=$(echo "$output" | grep -oE '[A-Za-z0-9]{40,}' | head -1)
if [[ -z "$room_key" ]]; then
    log_error "Could not extract room owner key from output. Please save it manually."
    echo "Output was:"
    echo "$output"
    read -p "Enter the room owner key: " room_key
fi

echo "$room_key" > "$ROOM_KEY_FILE"
log_success "Room created with key: $room_key"

# Step 4: User 1 creates invitation
log_step "User 1 (Alice): Creating invitation..."
output=$(riverctl --node-url "$FREENET_WS_URL_USER1" invite create "$room_key" 2>&1)
if [[ $? -ne 0 ]]; then
    log_error "Failed to create invitation"
    echo "$output"
    exit 1
fi

log_info "Invitation creation output:"
echo "$output"

# Extract invitation code - look for base64-like strings or specific patterns
invitation_code=$(echo "$output" | grep -oE '[A-Za-z0-9+/=]{40,}' | tail -1)
if [[ -z "$invitation_code" ]] || [[ "$invitation_code" == "$room_key" ]]; then
    log_error "Could not extract invitation code from output. Please save it manually."
    echo "Output was:"
    echo "$output"
    read -p "Enter the invitation code: " invitation_code
fi

echo "$invitation_code" > "$INVITATION_CODE_FILE"
log_success "Invitation created: $invitation_code"

# Step 5: User 2 accepts invitation
log_step "User 2 (Bob): Accepting invitation..."
output=$(RIVER_CONFIG_DIR="$USER2_CONFIG_DIR" riverctl --node-url "$FREENET_WS_URL_USER2" invite accept "$invitation_code" --nickname "$USER2_NICKNAME" 2>&1 || echo "ERROR")
if [[ "$output" == *"ERROR"* ]]; then
    log_error "Failed to accept invitation"
    echo "$output"
    exit 1
fi
log_success "User 2 joined the room"

# Wait a moment for synchronization
log_step "Waiting for synchronization..."
sleep 2

# Step 6: User 1 sends a message
log_step "User 1 (Alice): Sending message..."
if riverctl --node-url "$FREENET_WS_URL_USER1" message send "$room_key" "Hello from Alice!" 2>/dev/null; then
    log_success "Message sent by Alice"
else
    log_error "Failed to send message from Alice"
fi

# Step 7: User 2 sends a message
log_step "User 2 (Bob): Sending message..."
if RIVER_CONFIG_DIR="$USER2_CONFIG_DIR" riverctl --node-url "$FREENET_WS_URL_USER2" message send "$room_key" "Hi Alice, this is Bob!" 2>/dev/null; then
    log_success "Message sent by Bob"
else
    log_error "Failed to send message from Bob"
fi

# Wait for message propagation
log_step "Waiting for message propagation..."
sleep 3

# Step 8: Both users list messages
log_step "User 1 (Alice): Listing messages..."
echo -e "${YELLOW}--- Alice's View ---${NC}"
riverctl --node-url "$FREENET_WS_URL_USER1" message list "$room_key" || log_error "Failed to list messages for Alice"

echo ""
log_step "User 2 (Bob): Listing messages..."
echo -e "${YELLOW}--- Bob's View ---${NC}"
RIVER_CONFIG_DIR="$USER2_CONFIG_DIR" riverctl --node-url "$FREENET_WS_URL_USER2" message list "$room_key" || log_error "Failed to list messages for Bob"

echo ""
log_success "River test completed!"
log_info "Room key: $room_key"
log_info "Invitation code: $invitation_code"
log_info "User 1 config: $USER1_CONFIG_DIR"
log_info "User 2 config: $USER2_CONFIG_DIR"