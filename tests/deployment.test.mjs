import test from 'node:test';
import assert from 'node:assert/strict';
import {backendURLs} from '../static/deployment.mjs';

test('local and separately hosted API endpoints use the right websocket protocol',()=>{
  assert.deepEqual(backendURLs('', 'http://localhost:8086'),{http:'http://localhost:8086',ws:'ws://localhost:8086/ws'});
  assert.deepEqual(backendURLs('https://api.example.com/', 'https://perryong.github.io'),{http:'https://api.example.com',ws:'wss://api.example.com/ws'});
});
test('Pages requires a real HTTPS backend, not same-origin or mixed content',()=>{
  assert.throws(()=>backendURLs('', 'https://perryong.github.io'),/backend/i);
  assert.throws(()=>backendURLs('http://api.example.com', 'https://perryong.github.io'),/HTTPS/);
  assert.throws(()=>backendURLs('https://user:secret@api.example.com', 'https://perryong.github.io'),/credentials/i);
});
