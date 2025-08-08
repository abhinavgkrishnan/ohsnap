// detailed-stats.js - Show what's actually being processed
const { Client } = require('pg');

async function getDetailedStats() {
  const client = new Client({
    connectionString: 'postgres://shuttle:password@localhost:6541/shuttle'
  });
  
  // Message type names mapping
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
  
  try {
    await client.connect();
    
    console.log('📊 SNAPCHAIN SHUTTLE - DETAILED STATS');
    console.log('=====================================');
    console.log(`📅 ${new Date().toISOString()}`);
    console.log('');
    
    // Total progress summary
    console.log('🎯 OVERALL PROGRESS:');
    const totalMessages = await client.query('SELECT COUNT(*) as total FROM messages;');
    const totalCasts = await client.query('SELECT COUNT(*) as total FROM casts WHERE deleted_at IS NULL;');
    const totalReactions = await client.query('SELECT COUNT(*) as total FROM reactions WHERE deleted_at IS NULL;');
    const totalLinks = await client.query('SELECT COUNT(*) as total FROM links WHERE deleted_at IS NULL;');
    
    console.log(`  Total Messages Processed: ${parseInt(totalMessages.rows[0].total).toLocaleString()}`);
    console.log(`  Active Casts: ${parseInt(totalCasts.rows[0].total).toLocaleString()}`);
    console.log(`  Active Reactions: ${parseInt(totalReactions.rows[0].total).toLocaleString()}`);
    console.log(`  Active Links: ${parseInt(totalLinks.rows[0].total).toLocaleString()}`);
    
    // Quick performance check - messages in last 5 minutes
    const recentActivity = await client.query(`
      SELECT COUNT(*) as recent_count 
      FROM messages 
      WHERE timestamp > NOW() - INTERVAL '5 minutes';
    `);
    const recentCount = parseInt(recentActivity.rows[0].recent_count);
    const ratePerSecond = Math.round(recentCount / 300); // 5 minutes = 300 seconds
    
    if (recentCount > 0) {
      console.log(`  🔥 Recent Activity: ${recentCount} messages in last 5 minutes (${ratePerSecond} msg/sec)`);
    } else {
      console.log(`  ⏸️  No recent activity (may be processing older messages)`);
    }
    
    console.log('');
    
    // Message type distribution - fix the time filter since we might not have messages from last hour
    console.log('📈 MESSAGE TYPES IN MESSAGES TABLE:');
    const messageTypes = await client.query(`
      SELECT 
        type,
        COUNT(*) as count,
        MAX(timestamp) as latest_message
      FROM messages 
      GROUP BY type 
      ORDER BY count DESC;
    `);
    
    messageTypes.rows.forEach(row => {
      const typeName = typeNames[row.type] || `UNKNOWN_${row.type}`;
      console.log(`  ${typeName}: ${row.count.toLocaleString()} (latest: ${row.latest_message})`);
    });
    
    console.log('');
    
    // Custom table stats
    console.log('📋 CUSTOM TABLES:');
    
    // Tables with deleted_at (soft deletable)
    const softDeleteTables = ['casts', 'reactions', 'links'];
    
    for (const table of softDeleteTables) {
      try {
        const result = await client.query(`
          SELECT 
            COUNT(*) as total,
            COUNT(*) FILTER (WHERE deleted_at IS NULL) as active,
            MAX(created_at) as latest
          FROM ${table};
        `);
        
        const row = result.rows[0];
        console.log(`  ${table}: ${row.total} total, ${row.active} active (latest: ${row.latest})`);
        
      } catch (error) {
        console.log(`  ${table}: ERROR - ${error.message}`);
      }
    }
    
    // message_stats is different - it's just a log table, no soft deletes
    try {
      const statsResult = await client.query(`
        SELECT 
          COUNT(*) as total,
          MAX(created_at) as latest
        FROM message_stats;
      `);
      
      const row = statsResult.rows[0];
      console.log(`  message_stats: ${row.total} total (latest: ${row.latest})`);
      
      // Show message_stats breakdown
      const breakdown = await client.query(`
        SELECT 
          message_type,
          operation,
          COUNT(*) as count
        FROM message_stats 
        GROUP BY message_type, operation 
        ORDER BY count DESC 
        LIMIT 5;
      `);
      
      if (breakdown.rows.length > 0) {
        console.log(`    📊 Top operations:`);
        breakdown.rows.forEach(row => {
          const typeName = typeNames[row.message_type] || `TYPE_${row.message_type}`;
          console.log(`      ${typeName} ${row.operation}: ${row.count}`);
        });
      }
      
    } catch (error) {
      console.log(`  message_stats: ERROR - ${error.message}`);
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
    
    // Processing rate (recent activity)
    console.log('⚡ PROCESSING RATE (recent activity):');
    const rateQuery = await client.query(`
      SELECT 
        DATE_TRUNC('minute', timestamp) as minute,
        COUNT(*) as messages_per_minute
      FROM messages 
      WHERE timestamp > NOW() - INTERVAL '30 minutes'
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
