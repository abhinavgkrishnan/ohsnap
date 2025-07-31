// detailed-stats.js - Show what's actually being processed
const { Client } = require('pg');

async function getDetailedStats() {
  const client = new Client({
    connectionString: 'postgres://shuttle:password@localhost:6541/shuttle'
  });
  
  try {
    await client.connect();
    
    console.log('📊 SNAPCHAIN SHUTTLE - DETAILED STATS');
    console.log('=====================================');
    console.log(`📅 ${new Date().toISOString()}`);
    console.log('');
    
    // Message type distribution
    console.log('📈 MESSAGE TYPES IN MESSAGES TABLE:');
    const messageTypes = await client.query(`
      SELECT 
        type,
        COUNT(*) as count,
        MAX(timestamp) as latest_message
      FROM messages 
      WHERE timestamp > NOW() - INTERVAL '1 hour'
      GROUP BY type 
      ORDER BY count DESC;
    `);
    
    const typeNames = {
      1: 'CAST_ADD',
      2: 'CAST_REMOVE', 
      3: 'REACTION_ADD',
      4: 'REACTION_REMOVE',
      5: 'LINK_ADD',
      6: 'LINK_REMOVE',
      7: 'VERIFICATION_ADD_ETH_ADDRESS',
      8: 'VERIFICATION_REMOVE',
      11: 'USER_DATA_ADD'
    };
    
    messageTypes.rows.forEach(row => {
      const typeName = typeNames[row.type] || `UNKNOWN_${row.type}`;
      console.log(`  ${typeName}: ${row.count.toLocaleString()} (latest: ${row.latest_message})`);
    });
    
    console.log('');
    
    // Custom table stats
    console.log('📋 CUSTOM TABLES:');
    const tables = ['casts', 'reactions', 'links', 'message_stats'];
    
    for (const table of tables) {
      try {
        const result = await client.query(`
          SELECT 
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE "deletedAt" IS NULL) as active,
            MAX("createdAt") as latest
          FROM ${table};
        `);
        
        const row = result.rows[0];
        console.log(`  ${table}: ${row.total} total, ${row.active} active (latest: ${row.latest})`);
        
      } catch (error) {
        console.log(`  ${table}: ERROR - ${error.message}`);
      }
    }
    
    console.log('');
    
    // Database size
    console.log('💾 DATABASE SIZE:');
    const dbSize = await client.query(`
      SELECT 
        schemaname,
        tablename,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
        pg_total_relation_size(schemaname||'.'||tablename) as bytes
      FROM pg_tables 
      WHERE schemaname = 'public' 
      ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
    `);
    
    dbSize.rows.forEach(row => {
      console.log(`  ${row.tablename}: ${row.size}`);
    });
    
    console.log('');
    
    // Processing rate (last hour)
    console.log('⚡ PROCESSING RATE (last hour):');
    const rateQuery = await client.query(`
      SELECT 
        DATE_TRUNC('minute', timestamp) as minute,
        COUNT(*) as messages_per_minute
      FROM messages 
      WHERE timestamp > NOW() - INTERVAL '1 hour'
      GROUP BY DATE_TRUNC('minute', timestamp)
      ORDER BY minute DESC
      LIMIT 10;
    `);
    
    rateQuery.rows.forEach(row => {
      console.log(`  ${row.minute.toISOString()}: ${row.messages_per_minute} msg/min`);
    });
    
    if (rateQuery.rows.length > 0) {
      const avgRate = rateQuery.rows.reduce((sum, row) => sum + parseInt(row.messages_per_minute), 0) / rateQuery.rows.length;
      console.log(`  📊 Average: ${Math.round(avgRate)} msg/min (${Math.round(avgRate/60)} msg/sec)`);
    }
    
  } catch (error) {
    console.error('❌ Error getting stats:', error.message);
  } finally {
    await client.end();
  }
}

getDetailedStats();