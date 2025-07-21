#!/usr/bin/env node

const WebSocket = require('ws');
const crypto = require('crypto');

// Test River invitation flow to trigger contract operations on vega
const VEGA_URL = 'ws://vega.locut.us:50509/ws';
const TEST_CONTRACT_KEY = 'BcfxyjCH4snaknrBoCiqhYc9UFvmiJvhsp5d4L5DuvRa';

console.log('Testing River invitation flow to trigger contract operations on vega...');
console.log(`Connecting to: ${VEGA_URL}`);

const ws = new WebSocket(VEGA_URL);

ws.on('open', () => {
    console.log('Connected to vega WebSocket');
    
    // Send a GET request for River contract to trigger WASM execution
    const getMessage = {
        type: 'get',
        key: TEST_CONTRACT_KEY,
        fetch_contract: true,
        id: crypto.randomBytes(16).toString('hex')
    };
    
    console.log('Sending GET request:', getMessage);
    ws.send(JSON.stringify(getMessage));
    
    // Also send a PUT to trigger more WASM execution
    setTimeout(() => {
        const putMessage = {
            type: 'put',
            key: TEST_CONTRACT_KEY,
            state: {
                data: Buffer.from('test state data').toString('base64')
            },
            id: crypto.randomBytes(16).toString('hex')
        };
        
        console.log('Sending PUT request:', putMessage);
        ws.send(JSON.stringify(putMessage));
    }, 1000);
});

ws.on('message', (data) => {
    console.log('Received:', data.toString());
});

ws.on('error', (error) => {
    console.error('WebSocket error:', error);
});

ws.on('close', () => {
    console.log('WebSocket closed');
});

// Keep connection open for 30 seconds to see if it times out
setTimeout(() => {
    console.log('Test complete, closing connection');
    ws.close();
    process.exit(0);
}, 30000);