const fs = require('fs').promises;
const path = require('path');
const axios = require('axios');

async function loadFilteredBillsAndCallConfAPI() {
  try {
    console.log('Loading filtered bills data...');
    
    // Load assembly_filtered_bills_passed.json
    const filteredPath = path.join(__dirname, 'assembly_filtered_bills_passed.json');
    const content = await fs.readFile(filteredPath, 'utf8');
    const filteredData = JSON.parse(content);
    
    if (!filteredData.data || !Array.isArray(filteredData.data)) {
      throw new Error('No data array found in filtered bills file');
    }
    
    console.log(`Found ${filteredData.data.length} bills to process`);
    
    const apiResults = [];
    const API_KEY = '84d2bb829a0d413b97da6e9d2809db9e';
    const BASE_URL = 'https://open.assembly.go.kr/portal/openapi/VCONFBILLCONFLIST';
    
    // Process bills in batches to avoid overwhelming the API
    const batchSize = 10;
    for (let i = 0; i < filteredData.data.length; i += batchSize) {
      const batch = filteredData.data.slice(i, i + batchSize);
      
      console.log(`Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(filteredData.data.length / batchSize)}`);
      
      const batchPromises = batch.map(async (bill) => {
        try {
          const url = `${BASE_URL}?KEY=${API_KEY}&Type=json&pIndex=1&pSize=1000&BILL_ID=${bill.BILL_ID}`;
          
          console.log(`Calling CONFERENCE API for BILL_ID: ${bill.BILL_ID}`);
          
          const response = await axios.get(url, {
            timeout: 30000, // 30 second timeout
          });
          
          if (response.data && response.data.VCONFBILLCONFLIST) {
            console.log(`✓ Success for BILL_ID: ${bill.BILL_ID}`);
            
            return {
              BILL_ID: bill.BILL_ID,
              AGE: bill.AGE,
              api_response: response.data.VCONFBILLCONFLIST,
              status: 'success',
              timestamp: new Date().toISOString()
            };
          } else {
            console.log(`- No data for BILL_ID: ${bill.BILL_ID}`);
            return {
              BILL_ID: bill.BILL_ID,
              AGE: bill.AGE,
              api_response: null,
              status: 'no_data',
              timestamp: new Date().toISOString()
            };
          }
        } catch (error) {
          console.error(`✗ Error for BILL_ID ${bill.BILL_ID}:`, error.message);
          return {
            BILL_ID: bill.BILL_ID,
            AGE: bill.AGE,
            api_response: null,
            status: 'error',
            error: error.message,
            timestamp: new Date().toISOString()
          };
        }
      });
      
      const batchResults = await Promise.all(batchPromises);
      apiResults.push(...batchResults);
      
      // Add delay between batches to be respectful to the API
      if (i + batchSize < filteredData.data.length) {
        console.log('Waiting 2 seconds before next batch...');
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }
    
    // Compile results
    const successfulResults = apiResults.filter(result => result.status === 'success');
    const failedResults = apiResults.filter(result => result.status === 'error');
    const noDataResults = apiResults.filter(result => result.status === 'no_data');
    
    const compiledData = {
      summary: {
        total_bills_processed: apiResults.length,
        successful_calls: successfulResults.length,
        failed_calls: failedResults.length,
        no_data_calls: noDataResults.length,
        processed_date: new Date().toISOString()
      },
      results: apiResults
    };
    
    // Save compiled results
    console.log('Saving compiled results...');
    const outputPath = path.join(__dirname, 'assembly_bills_conference_api_results.json');
    
    try {
      const jsonString = JSON.stringify(compiledData, null, 2);
      console.log(`JSON string length: ${jsonString.length} characters`);
      
      await fs.writeFile(outputPath, jsonString, 'utf8');
      console.log(`Results saved successfully to: ${outputPath}`);
      
      // Verify file was created
      const stats = await fs.stat(outputPath);
      console.log(`File size: ${stats.size} bytes`);
      
    } catch (writeError) {
      console.error('Error saving results file:', writeError);
      
      // Try saving in chunks if main file fails
      const chunkSize = 100;
      for (let i = 0; i < apiResults.length; i += chunkSize) {
        const chunk = apiResults.slice(i, i + chunkSize);
        const chunkPath = path.join(__dirname, `assembly_bills_conf_chunk_${Math.floor(i / chunkSize) + 1}.json`);
        await fs.writeFile(chunkPath, JSON.stringify({ chunk_number: Math.floor(i / chunkSize) + 1, data: chunk }, null, 2), 'utf8');
        console.log(`Chunk ${Math.floor(i / chunkSize) + 1} saved to: ${chunkPath}`);
      }
    }
    
    console.log(`\nAPI calls completed!`);
    console.log(`Total processed: ${apiResults.length}`);
    console.log(`Successful: ${successfulResults.length}`);
    console.log(`Failed: ${failedResults.length}`);
    console.log(`No data: ${noDataResults.length}`);
    
    return compiledData;
    
  } catch (error) {
    console.error('Error in loadFilteredBillsAndCallConfAPI:', error);
    throw error;
  }
}

// Run the script if this file is executed directly
if (require.main === module) {
  loadFilteredBillsAndCallConfAPI()
    .then(results => {
      console.log(`\nProcessing completed successfully!`);
      console.log(`Final Summary: ${results.summary.total_bills_processed} bills processed, ${results.summary.successful_calls} successful API calls`);
    })
    .catch(error => {
      console.error('Script failed:', error);
      process.exit(1);
    });
}

module.exports = { loadFilteredBillsAndCallConfAPI };
