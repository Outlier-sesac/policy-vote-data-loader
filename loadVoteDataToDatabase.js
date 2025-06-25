require('dotenv').config();
const sql = require('mssql');
const fs = require('fs').promises;
const path = require('path');

// Database configuration
const config = {
  user: process.env.DB_USERNAME,
  password: process.env.DB_PASSWORD,
  server: process.env.DB_SERVER,
  database: process.env.DB_DATABASE,
  options: {
    encrypt: true,
    trustServerCertificate: false
  }
};

// Table schema for plenary session vote data
const voteTableSchema = `
  CREATE TABLE assembly_plenary_session_vote (
    id INT IDENTITY(1,1) PRIMARY KEY,
    HG_NM NVARCHAR(100),
    HJ_NM NVARCHAR(100),
    POLY_NM NVARCHAR(100),
    ORIG_NM NVARCHAR(100),
    MEMBER_NO NVARCHAR(50),
    POLY_CD NVARCHAR(50),
    ORIG_CD NVARCHAR(50),
    VOTE_DATE NVARCHAR(50),
    BILL_NO NVARCHAR(50),
    BILL_NAME NVARCHAR(500),
    BILL_ID NVARCHAR(100),
    LAW_TITLE NVARCHAR(500),
    CURR_COMMITTEE NVARCHAR(200),
    RESULT_VOTE_MOD NVARCHAR(50),
    DEPT_CD NVARCHAR(50),
    CURR_COMMITTEE_ID NVARCHAR(50),
    DISP_ORDER INT,
    BILL_URL NVARCHAR(1000),
    BILL_NAME_URL NVARCHAR(1000),
    SESSION_CD INT,
    CURRENTS_CD INT,
    AGE INT,
    MONA_CD NVARCHAR(50),
    created_at DATETIME2 DEFAULT GETDATE()
  )
`;

async function createVoteTable(pool) {
  console.log('Creating assembly_plenary_session_vote table...');
  try {
    // Check if table exists
    const checkResult = await pool.request()
      .input('tableName', sql.NVarChar, 'assembly_plenary_session_vote')
      .query(`
        SELECT COUNT(*) as count 
        FROM INFORMATION_SCHEMA.TABLES 
        WHERE TABLE_NAME = @tableName
      `);
    
    if (checkResult.recordset[0].count === 0) {
      await pool.request().query(voteTableSchema);
      console.log('Table assembly_plenary_session_vote created.');
    } else {
      console.log('Table assembly_plenary_session_vote already exists.');
    }
  } catch (error) {
    console.error('Error creating vote table:', error);
    throw error;
  }
}

async function insertVoteData(pool, voteItem, originalBillId, originalAge, apiStatus) {
  try {
    await pool.request()
      .input('HG_NM', sql.NVarChar(100), voteItem.HG_NM)
      .input('HJ_NM', sql.NVarChar(100), voteItem.HJ_NM)
      .input('POLY_NM', sql.NVarChar(100), voteItem.POLY_NM)
      .input('ORIG_NM', sql.NVarChar(100), voteItem.ORIG_NM)
      .input('MEMBER_NO', sql.NVarChar(50), voteItem.MEMBER_NO)
      .input('POLY_CD', sql.NVarChar(50), voteItem.POLY_CD)
      .input('ORIG_CD', sql.NVarChar(50), voteItem.ORIG_CD)
      .input('VOTE_DATE', sql.NVarChar(50), voteItem.VOTE_DATE)
      .input('BILL_NO', sql.NVarChar(50), voteItem.BILL_NO)
      .input('BILL_NAME', sql.NVarChar(500), voteItem.BILL_NAME)
      .input('BILL_ID', sql.NVarChar(100), voteItem.BILL_ID)
      .input('LAW_TITLE', sql.NVarChar(500), voteItem.LAW_TITLE)
      .input('CURR_COMMITTEE', sql.NVarChar(200), voteItem.CURR_COMMITTEE)
      .input('RESULT_VOTE_MOD', sql.NVarChar(50), voteItem.RESULT_VOTE_MOD)
      .input('DEPT_CD', sql.NVarChar(50), voteItem.DEPT_CD)
      .input('CURR_COMMITTEE_ID', sql.NVarChar(50), voteItem.CURR_COMMITTEE_ID)
      .input('DISP_ORDER', sql.Int, voteItem.DISP_ORDER)
      .input('BILL_URL', sql.NVarChar(1000), voteItem.BILL_URL)
      .input('BILL_NAME_URL', sql.NVarChar(1000), voteItem.BILL_NAME_URL)
      .input('SESSION_CD', sql.Int, voteItem.SESSION_CD)
      .input('CURRENTS_CD', sql.Int, voteItem.CURRENTS_CD)
      .input('AGE', sql.Int, voteItem.AGE)
      .input('MONA_CD', sql.NVarChar(50), voteItem.MONA_CD)
      .query(`
        INSERT INTO assembly_plenary_session_vote (
          HG_NM, HJ_NM, POLY_NM, ORIG_NM, MEMBER_NO, POLY_CD, ORIG_CD, VOTE_DATE,
          BILL_NO, BILL_NAME, BILL_ID, LAW_TITLE, CURR_COMMITTEE, RESULT_VOTE_MOD,
          DEPT_CD, CURR_COMMITTEE_ID, DISP_ORDER, BILL_URL, BILL_NAME_URL,
          SESSION_CD, CURRENTS_CD, AGE, MONA_CD
        ) VALUES (
          @HG_NM, @HJ_NM, @POLY_NM, @ORIG_NM, @MEMBER_NO, @POLY_CD, @ORIG_CD, @VOTE_DATE,
          @BILL_NO, @BILL_NAME, @BILL_ID, @LAW_TITLE, @CURR_COMMITTEE, @RESULT_VOTE_MOD,
          @DEPT_CD, @CURR_COMMITTEE_ID, @DISP_ORDER, @BILL_URL, @BILL_NAME_URL,
          @SESSION_CD, @CURRENTS_CD, @AGE, @MONA_CD
        )
      `);
  } catch (error) {
    console.error('Error inserting vote record:', error.message);
    throw error;
  }
}

async function loadAPIResultsData() {
  try {
    console.log('Loading API results data...');
    
    let apiData;
    let dataSource;
    
    // Try to load main results file first
    try {
      const mainPath = path.join(__dirname, 'assembly_bills_api_results.json');
      const content = await fs.readFile(mainPath, 'utf8');
      apiData = JSON.parse(content);
      dataSource = 'assembly_bills_api_results.json';
      console.log('Loaded data from main results file');
    } catch (error) {
      // Fallback to temp file
      try {
        const tempPath = path.join(__dirname, 'assembly_bills_api_results_temp.json');
        const content = await fs.readFile(tempPath, 'utf8');
        apiData = JSON.parse(content);
        dataSource = 'assembly_bills_api_results_temp.json';
        console.log('Loaded data from temp results file');
      } catch (tempError) {
        throw new Error('Neither main nor temp API results file found');
      }
    }
    
    if (!apiData.results || !Array.isArray(apiData.results)) {
      throw new Error('No results array found in API data');
    }
    
    console.log(`Data source: ${dataSource}`);
    console.log(`Found ${apiData.results.length} API result records`);
    
    return apiData;
    
  } catch (error) {
    console.error('Error loading API results data:', error);
    throw error;
  }
}

async function processAPIResults(pool, apiData) {
  console.log('Processing API results for vote data...');
  
  let processedCount = 0;
  let successfulInserts = 0;
  let voteRecordsFound = 0;

  // let flag = false;
  
  for (const result of apiData.results) {
    processedCount++;

    // TODO 오류 발생한 BILL_ID 기준으로 작업 재시작
    // console.log('result.BILL_ID', result.BILL_ID)
    // if (result.BILL_ID == 'PRC_W1R7Y0O8M0J9E1W7E4M3U4Q1K2V5X5') flag = true
    // if (flag == false) continue;
    
    if (processedCount % 100 === 0) {
      console.log(`Processed ${processedCount}/${apiData.results.length} records...`);
    }
    
    // Only process successful API responses
    if (result.status !== 'success' || !result.api_response) {
      continue;
    }
    
    try {
      // Check if api_response has the expected structure
      if (!Array.isArray(result.api_response) || result.api_response.length < 2) {
        continue;
      }
      
      // Get the second element (index 1) from api_response array
      const voteData = result.api_response[1];
      
      if (!voteData || !Array.isArray(voteData.row)) {
        continue;
      }
      
      // Process each vote record in the row array
      for (const voteItem of voteData.row) {
        voteRecordsFound++;
        
        try {
          await insertVoteData(
            pool, 
            voteItem, 
            result.BILL_ID, 
            result.AGE, 
            result.status
          );
          successfulInserts++;
        } catch (insertError) {
          console.error(`Error inserting vote record for BILL_ID ${result.BILL_ID}:`, insertError.message);
        }
      }
      
    } catch (error) {
      console.error(`Error processing result for BILL_ID ${result.BILL_ID}:`, error.message);
    }
  }
  
  console.log(`\nProcessing completed:`);
  console.log(`- Total API results processed: ${processedCount}`);
  console.log(`- Vote records found: ${voteRecordsFound}`);
  console.log(`- Successful database insertions: ${successfulInserts}`);
  
  return {
    processedCount,
    voteRecordsFound,
    successfulInserts
  };
}

async function main() {
  let pool;
  
  try {
    console.log('Connecting to Azure SQL Database...');
    pool = await sql.connect(config);
    console.log('Connected successfully!');
    
    // Create vote table
    await createVoteTable(pool);
    
    // Load API results data
    const apiData = await loadAPIResultsData();
    
    // Process and insert vote data
    const results = await processAPIResults(pool, apiData);
    
    console.log('\nData loading completed successfully!');
    console.log(`Summary: ${results.successfulInserts} vote records inserted from ${results.voteRecordsFound} found records`);
    
  } catch (error) {
    console.error('Error:', error);
    process.exit(1);
  } finally {
    if (pool) {
      await pool.close();
      console.log('Database connection closed.');
    }
  }
}

if (require.main === module) {
  main();
}

module.exports = { main, loadAPIResultsData, processAPIResults };
