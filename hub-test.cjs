// Create a simple test file: hub-test.js
const { getInsecureHubRpcClient } = require('@farcaster/hub-nodejs');

async function testHub() {
  console.log('🔍 Testing Snapchain hub connectivity...');
  
  const client = getInsecureHubRpcClient('localhost:3381');
  
  try {
    // Test basic connectivity
    const info = await client.getInfo({});
    if (info.isOk()) {
      console.log('✅ Hub info:', JSON.stringify(info.value, null, 2));
    } else {
      console.log('❌ Failed to get hub info:', info.error);
      return;
    }
    
    // Test subscription (just connect, don't process)
    console.log('🔍 Testing subscription...');
    const subscribeResult = await client.subscribe({
      eventTypes: [1, 2, 3, 6, 9], // Same event types as your shuttle
      shardIndex: 0,
      // Don't specify fromId to start from current
    });
    
    if (subscribeResult.isOk()) {
      console.log('✅ Subscription successful, listening for 10 seconds...');
      const stream = subscribeResult.value;
      
      let eventCount = 0;
      const timeout = setTimeout(() => {
        console.log(`✅ Received ${eventCount} events in 10 seconds`);
        stream.cancel();
        client.$.close();
      }, 10000);
      
      stream.on('data', (event) => {
        eventCount++;
        if (eventCount <= 3) {
          console.log(`📨 Event ${eventCount}:`, {
            id: event.id,
            type: event.type,
            shardIndex: event.shardIndex
          });
        }
      });
      
      stream.on('error', (error) => {
        console.log('❌ Stream error:', error.message);
        clearTimeout(timeout);
        client.$.close();
      });
      
      stream.on('end', () => {
        console.log('📡 Stream ended');
        clearTimeout(timeout);
        client.$.close();
      });
      
    } else {
      console.log('❌ Subscription failed:', subscribeResult.error);
    }
    
  } catch (error) {
    console.log('❌ Test failed:', error.message);
    client.$.close();
  }
}

testHub();