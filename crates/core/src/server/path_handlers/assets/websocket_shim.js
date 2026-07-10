(function () {
  'use strict';
  var wsInstances = new Map();
  var idCounter = 0;

  function FreenetWebSocket(url, protocols) {
    this._id = '__fws_' + ++idCounter;
    this.url = url;
    this.readyState = 0;
    this.bufferedAmount = 0;
    this.extensions = '';
    this.protocol = '';
    this.binaryType = 'blob';
    this.onopen = null;
    this.onmessage = null;
    this.onclose = null;
    this.onerror = null;
    this._listeners = {};
    wsInstances.set(this._id, this);
    window.parent.postMessage(
      {
        __freenet_ws__: true,
        type: 'open',
        id: this._id,
        url: url,
        protocols: protocols,
      },
      '*',
    );
  }
  FreenetWebSocket.CONNECTING = 0;
  FreenetWebSocket.OPEN = 1;
  FreenetWebSocket.CLOSING = 2;
  FreenetWebSocket.CLOSED = 3;
  FreenetWebSocket.prototype.send = function (data) {
    if (this.readyState !== 1)
      throw new DOMException('WebSocket is not open', 'InvalidStateError');
    var payload = data;
    var transfer = [];
    if (data instanceof ArrayBuffer) {
      transfer = [data];
    } else if (ArrayBuffer.isView(data)) {
      // wasm-bindgen hands us a Uint8Array that may be a view into WASM linear
      // memory; copy exactly this view's window into a fresh buffer so we can
      // transfer ownership without detaching that. Use data.buffer.slice (not
      // data.slice) so a DataView — which has no .slice — is handled too.
      var buf = data.buffer.slice(
        data.byteOffset,
        data.byteOffset + data.byteLength,
      );
      payload = new Uint8Array(buf);
      transfer = [buf];
    }
    window.parent.postMessage(
      {
        __freenet_ws__: true,
        type: 'send',
        id: this._id,
        data: payload,
      },
      '*',
      transfer,
    );
  };
  FreenetWebSocket.prototype.close = function (code, reason) {
    if (this.readyState >= 2) return;
    this.readyState = 2;
    window.parent.postMessage(
      {
        __freenet_ws__: true,
        type: 'close',
        id: this._id,
        code: code,
        reason: reason,
      },
      '*',
    );
  };
  FreenetWebSocket.prototype.addEventListener = function (type, listener) {
    if (!this._listeners[type]) this._listeners[type] = [];
    this._listeners[type].push(listener);
  };
  FreenetWebSocket.prototype.removeEventListener = function (type, listener) {
    if (!this._listeners[type]) return;
    this._listeners[type] = this._listeners[type].filter(function (l) {
      return l !== listener;
    });
  };
  FreenetWebSocket.prototype.dispatchEvent = function (event) {
    var handler = this['on' + event.type];
    if (handler) handler.call(this, event);
    var listeners = this._listeners[event.type];
    if (listeners)
      for (var i = 0; i < listeners.length; i++) listeners[i].call(this, event);
    return true;
  };

  window.addEventListener('message', function (event) {
    // Only accept messages from the parent shell page
    if (event.source !== window.parent) return;
    var msg = event.data;
    if (!msg || !msg.__freenet_ws__) return;
    var ws = wsInstances.get(msg.id);
    if (!ws) return;
    switch (msg.type) {
      case 'open':
        ws.readyState = 1;
        ws.dispatchEvent(new Event('open'));
        break;
      case 'message':
        var data = msg.data;
        if (ws.binaryType === 'blob' && data instanceof ArrayBuffer)
          data = new Blob([data]);
        ws.dispatchEvent(new MessageEvent('message', { data: data }));
        break;
      case 'close':
        ws.readyState = 3;
        ws.dispatchEvent(
          new CloseEvent('close', {
            code: msg.code,
            reason: msg.reason,
            wasClean: true,
          }),
        );
        wsInstances.delete(msg.id);
        break;
      case 'error':
        ws.dispatchEvent(new Event('error'));
        break;
    }
  });

  window.WebSocket = FreenetWebSocket;
  if (typeof globalThis !== 'undefined')
    globalThis.WebSocket = FreenetWebSocket;
})();
