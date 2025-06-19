# Playwright Notes for Testing River

## Key Learnings

### 1. Filling Input Fields in Dioxus Apps
- **Issue**: Standard `playwright_fill` selector may not work with Dioxus-generated inputs
- **Solution**: Use JavaScript evaluation to directly set values and dispatch events:
```javascript
(() => {
  const inputs = document.querySelectorAll('.modal.is-active input');
  if (inputs[0]) {
    inputs[0].value = 'Test Room Debug';
    inputs[0].dispatchEvent(new Event('input', { bubbles: true }));
    inputs[0].dispatchEvent(new Event('change', { bubbles: true }));
    return 'Filled room name';
  }
  return 'Input not found';
})()
```

### 2. Clicking Buttons in Modals
- **Issue**: Multiple buttons with same text can cause conflicts; Playwright may try to click hidden buttons
- **Solution**: Use JavaScript to find and click the visible button:
```javascript
(() => {
  const modalButtons = document.querySelectorAll('.modal.is-active button');
  for (const btn of modalButtons) {
    if (btn.textContent.trim() === 'Create Room' && btn.offsetParent !== null) {
      btn.click();
      return 'Clicked Create Room';
    }
  }
  return 'Button not found';
})()
```

### 3. Working with Dynamic DOM
- **Issue**: Dioxus generates dynamic `data-dioxus-id` attributes that change
- **Solution**: Use semantic selectors like class names, text content, or structural selectors

### 4. Input Values May Reset
- **Issue**: Input values sometimes clear after clicking (noticed room name was empty after first attempt)
- **Solution**: Always verify input values before submitting forms

### 5. Console Logs are Essential
- Use `playwright_console_logs` to monitor River's debug output
- Search for specific terms like "PUT", "room", "create" to track operations
- River logs are prefixed with color formatting (e.g., `%cINFO%c`)

## Useful Playwright Commands for River

1. **Get all visible buttons**:
```javascript
Array.from(document.querySelectorAll('button')).map(btn => ({
  text: btn.textContent.trim(),
  visible: btn.offsetParent !== null,
  className: btn.className
}))
```

2. **Check modal state**:
```javascript
(() => {
  const modal = document.querySelector('.modal.is-active');
  return modal ? 'Modal is still open' : 'Modal is closed';
})()
```

3. **Get modal inputs with labels**:
```javascript
(() => {
  const modal = document.querySelector('.modal.is-active');
  if (modal) {
    const labels = Array.from(modal.querySelectorAll('label')).map(l => l.textContent.trim());
    const inputs = Array.from(modal.querySelectorAll('input')).map((input, idx) => ({
      index: idx,
      value: input.value
    }));
    return { labels, inputs };
  }
  return null;
})()
```

## Common Patterns

1. **Wait for operations**: Use promises with setTimeout when needed
2. **Check for loading states**: Look for text like "Creating", "Loading", "Subscribing"
3. **Monitor WebSocket**: Console logs show WebSocket connection status
4. **Screenshot at key points**: Helps debug visual state

## River-Specific Notes

- River uses Dioxus which generates dynamic IDs
- Modal operations need special handling due to multiple button instances
- Input events need both 'input' and 'change' dispatching for proper state updates
- Console logs provide detailed WebSocket and room synchronization info

## Reproducing the Invitation Bug

1. Navigate to River URL
2. Create a room with User1
3. Click "Invite Member" and copy the invitation URL
4. Navigate to the invitation URL in the same browser window
5. Fill in nickname for User2
6. Click Accept
7. **Bug appears**: Modal shows "Subscribing to room..." and hangs indefinitely

The bug manifests after clicking Accept - the modal immediately changes to show "Subscribing to room..." without Accept/Decline buttons, indicating the click was processed but the subscription process hangs.

## Important Findings

1. **Room Not Persisted**: Created rooms don't persist after page refresh - they only exist in UI state
2. **No PUT in Freenet Logs**: Room creation doesn't generate visible PUT operations in Freenet logs
3. **Browser Connection**: Playwright browser connection can be lost during long operations
4. **Console Logs Are Key**: River's console logs provide detailed information about:
   - Contract IDs (e.g., `9udqzmrhWXPUbNoukJXuabzi34YSzmijjv8fDEf5Kq3z`)
   - Room member IDs (e.g., `MemberId(QAZBX566)`)
   - GET request attempts
   - WebSocket timeout errors