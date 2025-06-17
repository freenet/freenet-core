// Test River connection with updated gateways
const testRiverConnection = async () => {
    console.log('Testing River connection with v0.1.10 gateways...');
    
    // Test both gateways
    const gateways = [
        'https://vega.river.freenet.org',
        'https://technic.river.freenet.org'
    ];
    
    for (const gateway of gateways) {
        console.log(`\nTesting ${gateway}...`);
        
        try {
            // Open River app
            const page = await mcp.puppeteer.navigate(gateway);
            await page.waitForTimeout(3000);
            
            // Take screenshot
            await mcp.puppeteer.screenshot(page, `river-${gateway.includes('vega') ? 'vega' : 'technic'}-initial.png`);
            
            // Look for River UI elements
            const roomInput = await page.$('input[placeholder*="room" i]');
            const createButton = await page.$('button');
            
            if (roomInput && createButton) {
                console.log('River UI loaded successfully');
                
                // Try to create a room
                const roomName = `test-room-${Date.now()}`;
                await roomInput.type(roomName);
                await createButton.click();
                
                console.log('Creating room...');
                
                // Wait for connection
                await page.waitForTimeout(10000);
                
                // Take screenshot of result
                await mcp.puppeteer.screenshot(page, `river-${gateway.includes('vega') ? 'vega' : 'technic'}-result.png`);
                
                // Check if we're still on the initial page or if we've entered the room
                const stillOnInitialPage = await page.$('input[placeholder*="room" i]');
                
                if (!stillOnInitialPage) {
                    console.log('✅ Successfully entered room!');
                } else {
                    console.log('❌ Still on initial page - connection may have failed');
                }
            } else {
                console.log('❌ Could not find River UI elements');
            }
            
            await mcp.puppeteer.closePage(page);
            
        } catch (error) {
            console.error(`Error testing ${gateway}:`, error);
        }
    }
    
    console.log('\nTest complete');
};

// Run the test
testRiverConnection();