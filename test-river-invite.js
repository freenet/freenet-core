// Test script for River room invitation flow
// This script creates a room and tests the member synchronization

const RIVER_URL = 'http://127.0.0.1:50509/v1/contract/web/BcfxyjCH4snaknrBoCiqhYc9UFvmiJvhsp5d4L5DuvRa/';

async function sleep(ms) {
    return new Promise(resolve => setTimeout(resolve, ms));
}

async function testRiverInvite() {
    console.log('Starting River invitation test...');
    
    // Step 1: Navigate to River in first tab (room creator)
    console.log('1. Opening River for room creator (Alice)...');
    await playwright.navigate({ url: RIVER_URL });
    await sleep(2000);
    
    // Step 2: Create a room
    console.log('2. Creating room...');
    // Click create room button in sidebar
    await playwright.click({ selector: '.room-actions button.create' });
    await sleep(500);
    
    // Fill in room details - target the last modal (most recent)
    await playwright.fill({ 
        selector: '.modal:last-of-type .field:has(label:has-text("Room Name")) input' ,
        value: 'Test Room ' + Date.now()
    });
    await playwright.fill({ 
        selector: '.modal:last-of-type .field:has(label:has-text("Your Nickname")) input',
        value: 'Alice'
    });
    
    // Click the create button in the last modal
    await playwright.click({ selector: '.modal:last-of-type button.is-primary' });
    await sleep(3000);
    
    console.log('3. Room created, taking screenshot...');
    await playwright.screenshot({ name: 'room-created', fullPage: true });
    
    // Step 3: Create invite link
    console.log('4. Creating invite link...');
    await playwright.click({ selector: '.member-actions button.invite' });
    await sleep(1000);
    
    // Get the invite link
    const inviteLinkElement = await playwright.evaluate({ 
        script: `document.querySelector('.modal.is-active input[readonly]')?.value || ''`
    });
    
    console.log('5. Invite link:', inviteLinkElement);
    
    if (!inviteLinkElement) {
        console.error('Failed to get invite link!');
        return;
    }
    
    // Step 4: Open invite link in new tab
    console.log('6. Opening invite link in new tab for Bob...');
    await playwright.clickAndSwitchTab({ selector: 'body' }); // Click body to ensure we can switch tabs
    await playwright.navigate({ url: inviteLinkElement });
    await sleep(3000);
    
    console.log('7. Invite page loaded, taking screenshot...');
    await playwright.screenshot({ name: 'invite-page', fullPage: true });
    
    // Step 5: Accept invitation
    console.log('8. Accepting invitation...');
    // Fill in nickname
    await playwright.fill({
        selector: '.modal.is-active input[type="text"]',
        value: 'Bob'
    });
    
    // Click accept
    await playwright.click({ selector: '.modal.is-active button:has-text("Accept")' });
    
    console.log('9. Waiting for room to load...');
    await sleep(5000);
    
    console.log('10. Taking screenshot of Bob\'s view...');
    await playwright.screenshot({ name: 'bob-room-view', fullPage: true });
    
    // Step 6: Check member list in Bob's view
    const bobMemberList = await playwright.evaluate({
        script: `Array.from(document.querySelectorAll('.member-list-list li')).map(li => li.textContent.trim())`
    });
    console.log('11. Members visible to Bob:', bobMemberList);
    
    // Step 7: Switch back to Alice's tab
    console.log('12. Switching back to Alice\'s tab...');
    await playwright.goBack();
    await sleep(2000);
    
    // Step 8: Check member list in Alice's view
    const aliceMemberList = await playwright.evaluate({
        script: `Array.from(document.querySelectorAll('.member-list-list li')).map(li => li.textContent.trim())`
    });
    console.log('13. Members visible to Alice:', aliceMemberList);
    
    console.log('14. Taking final screenshot of Alice\'s view...');
    await playwright.screenshot({ name: 'alice-final-view', fullPage: true });
    
    // Analysis
    console.log('\n=== Test Results ===');
    console.log('Alice sees members:', aliceMemberList);
    console.log('Bob sees members:', bobMemberList);
    
    if (aliceMemberList.length === 2 && bobMemberList.length === 2) {
        console.log('✅ SUCCESS: Both users see both members!');
    } else {
        console.log('❌ FAIL: Member lists are not synchronized properly');
        console.log('Expected: Both users should see 2 members (Alice and Bob)');
    }
}

// Run the test
testRiverInvite().catch(console.error);