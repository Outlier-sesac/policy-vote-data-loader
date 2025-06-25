const fs = require('fs').promises;
const path = require('path');
const axios = require('axios');

async function processBillsData() {
  try {
    console.log('Starting to process assembly bills data...');
    
    // Get all files in the current directory
    const files = await fs.readdir(__dirname);
    
    // Filter files that start with 'assembly_bills_age' and end with '.json'
    const billsFiles = files.filter(file => 
      file.startsWith('assembly_bills_age') && file.endsWith('.json')
    );
    
    console.log(`Found ${billsFiles.length} assembly bills files to process.`);
    
    const filteredResults = [];
    
    for (const file of billsFiles) {
      console.log(`Processing file: ${file}`);
      
      try {
        const filePath = path.join(__dirname, file);
        const content = await fs.readFile(filePath, 'utf8');
        const jsonData = JSON.parse(content);
        
        if (!jsonData.data || !Array.isArray(jsonData.data)) {
          console.log(`Skipping ${file} - no data array found`);
          continue;
        }
        
        // Filter data where PROC_RESULT is '원안가결' or '수정가결'
        const filtered = jsonData.data.filter(item => 
          (item.PROC_RESULT === '원안가결' || item.PROC_RESULT === '수정가결')
          && Number(item.AGE) >= 17
        );
        
        // Extract BILL_ID and AGE
        const extracted = filtered.map(item => ({
          BILL_ID: item.BILL_ID,
          AGE: item.AGE
        }));
        
        filteredResults.push(...extracted);
        
        console.log(`Found ${filtered.length} matching records in ${file}`);
        
      } catch (error) {
        console.error(`Error processing file ${file}:`, error.message);
      }
    }
    
    // Remove duplicates based on BILL_ID
    const uniqueResults = filteredResults.filter((item, index, array) => 
      array.findIndex(i => i.BILL_ID === item.BILL_ID) === index
    );
    
    console.log(`Total filtered records: ${filteredResults.length}`);
    console.log(`Unique records after deduplication: ${uniqueResults.length}`);
    
    // Save results to JSON file
    const outputData = {
      total_count: uniqueResults.length,
      filtered_date: new Date().toISOString(),
      filter_criteria: "PROC_RESULT = '원안가결' OR '수정가결'",
      data: uniqueResults
    };
    
    const outputPath = path.join(__dirname, 'assembly_filtered_bills_passed.json');
    await fs.writeFile(outputPath, JSON.stringify(outputData, null, 2), 'utf8');
    
    console.log(`Results saved to: ${outputPath}`);
    console.log('Processing completed successfully!');
    
    return uniqueResults;
    
  } catch (error) {
    console.error('Error in processBillsData:', error);
    throw error;
  }
}

async function loadFilteredBillsAndCallAPI() {
  try {
    console.log('Loading filtered bills data...');
    
    // Load filtered_assembly_bills.json or filtered_bills_passed.json
    let filteredData;
    try {
      const filteredPath = path.join(__dirname, 'assembly_filtered_bills_passed.json');
      const content = await fs.readFile(filteredPath, 'utf8');
      filteredData = JSON.parse(content);
    } catch (error) {
      // Fallback to filtered_bills_passed.json if filtered_assembly_bills.json doesn't exist
      const filteredPath = path.join(__dirname, 'assembly_filtered_passed_bills.json');
      const content = await fs.readFile(filteredPath, 'utf8');
      filteredData = JSON.parse(content);
    }
    
    if (!filteredData.data || !Array.isArray(filteredData.data)) {
      throw new Error('No data array found in filtered bills file');
    }
    
    console.log(`Found ${filteredData.data.length} bills to process`);
    
    // Check for existing API results to avoid duplicates
    let existingResults = [];
    try {
      const existingPath = path.join(__dirname, 'assembly_bills_api_results.json');
      const existingContent = await fs.readFile(existingPath, 'utf8');
      const existingData = JSON.parse(existingContent);
      existingResults = existingData.results || [];
      console.log(`Found ${existingResults.length} existing API results`);
    } catch (error) {
      console.log('No existing API results found, starting fresh');
    }
    
    // Filter out bills that already have API results
    const billsToProcess = filteredData.data.filter(bill => {
      const exists = existingResults.some(result => 
        result.BILL_ID === bill.BILL_ID && result.AGE === bill.AGE
      );
      return !exists;
    });
    
    console.log(`After removing duplicates: ${billsToProcess.length} bills need API calls`);
    
    if (billsToProcess.length === 0) {
      console.log('All bills already have API results. No new calls needed.');
      return {
        summary: {
          total_bills_processed: existingResults.length,
          successful_calls: existingResults.filter(r => r.status === 'success').length,
          failed_calls: existingResults.filter(r => r.status === 'error').length,
          no_data_calls: existingResults.filter(r => r.status === 'no_data').length,
          processed_date: new Date().toISOString(),
          note: 'No new API calls made - all bills already processed'
        },
        results: existingResults
      };
    }
    
    const apiResults = [...existingResults]; // Start with existing results
    const API_KEY = '84d2bb829a0d413b97da6e9d2809db9e';
    const BASE_URL = 'https://open.assembly.go.kr/portal/openapi/nojepdqqaweusdfbi';
    
    // Process only new bills in batches
    const batchSize = 10;
    for (let i = 0; i < billsToProcess.length; i += batchSize) {
      const batch = billsToProcess.slice(i, i + batchSize);
      
      console.log(`Processing batch ${Math.floor(i / batchSize) + 1}/${Math.ceil(billsToProcess.length / batchSize)} (${batch.length} bills)`);
      
      const batchPromises = batch.map(async (bill) => {
        try {
          const url = `${BASE_URL}?KEY=${API_KEY}&Type=json&pIndex=1&pSize=1000&BILL_ID=${bill.BILL_ID}&AGE=${bill.AGE}`;
          
          console.log(`Calling API for BILL_ID: ${bill.BILL_ID}, AGE: ${bill.AGE}`);
          
          const response = await axios.get(url, {
            timeout: 30000, // 30 second timeout
          });
          
          if (response.data && response.data.nojepdqqaweusdfbi) {
            console.log(`✓ Success for BILL_ID: ${bill.BILL_ID}`);
            
            return {
              BILL_ID: bill.BILL_ID,
              AGE: bill.AGE,
              api_response: response.data.nojepdqqaweusdfbi,
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
      
      // Save intermediate results after each batch
      const intermediateData = {
        summary: {
          total_bills_processed: apiResults.length,
          successful_calls: apiResults.filter(result => result.status === 'success').length,
          failed_calls: apiResults.filter(result => result.status === 'error').length,
          no_data_calls: apiResults.filter(result => result.status === 'no_data').length,
          processed_date: new Date().toISOString(),
          batch_completed: Math.floor(i / batchSize) + 1
        },
        results: apiResults
      };
      
      try {
        const intermediatePath = path.join(__dirname, 'assembly_bills_api_results_temp.json');
        await fs.writeFile(intermediatePath, JSON.stringify(intermediateData, null, 2), 'utf8');
        console.log(`Intermediate results saved (${apiResults.length} total results)`);
      } catch (intermediateError) {
        console.warn(`Warning: Failed to save intermediate results:`, intermediateError.message);
        console.log(`Continuing with batch processing...`);
      }
      
      // Add delay between batches
      if (i + batchSize < billsToProcess.length) {
        console.log('Waiting 2 seconds before next batch...');
        await new Promise(resolve => setTimeout(resolve, 2000));
      }
    }
    
    // Final verification - check for any duplicates in results
    const uniqueResults = [];
    const seenBills = new Set();
    
    for (const result of apiResults) {
      const key = `${result.BILL_ID}_${result.AGE}`;
      if (!seenBills.has(key)) {
        seenBills.add(key);
        uniqueResults.push(result);
      } else {
        console.warn(`Duplicate found and removed: BILL_ID ${result.BILL_ID}, AGE ${result.AGE}`);
      }
    }
    
    // Compile results
    const compiledData = {
      summary: {
        total_bills_processed: uniqueResults.length,
        successful_calls: uniqueResults.filter(result => result.status === 'success').length,
        failed_calls: uniqueResults.filter(result => result.status === 'error').length,
        no_data_calls: uniqueResults.filter(result => result.status === 'no_data').length,
        processed_date: new Date().toISOString(),
        duplicates_removed: apiResults.length - uniqueResults.length
      },
      results: uniqueResults
    };
    
    // Save compiled results
    console.log('Saving compiled results...');
    const outputPath = path.join(__dirname, 'assembly_bills_api_results.json');
    
    try {
      // Convert to JSON string first to check if there are any issues
      let jsonString;
      try {
        jsonString = JSON.stringify(compiledData, null, 2);
        console.log(`JSON string length: ${jsonString.length} characters`);
      } catch (stringifyError) {
        console.warn('Warning: JSON stringify failed, trying without formatting:', stringifyError.message);
        try {
          jsonString = JSON.stringify(compiledData);
          console.log(`JSON string created without formatting. Length: ${jsonString.length} characters`);
        } catch (secondStringifyError) {
          console.error('Critical: JSON stringify failed completely:', secondStringifyError.message);
          console.log('Saving summary only and continuing...');
          
          // Save just the summary as fallback
          const summaryPath = path.join(__dirname, 'assembly_bills_summary_fallback.json');
          await fs.writeFile(summaryPath, JSON.stringify(compiledData.summary, null, 2), 'utf8');
          console.log(`Summary saved to: ${summaryPath}`);
          
          // Return the data even if saving failed
          console.log(`\nAPI calls completed despite saving issues!`);
          console.log(`Total processed: ${uniqueResults.length}`);
          console.log(`Successful: ${compiledData.summary.successful_calls}`);
          console.log(`Failed: ${compiledData.summary.failed_calls}`);
          console.log(`No data: ${compiledData.summary.no_data_calls}`);
          
          return compiledData;
        }
      }
      
      await fs.writeFile(outputPath, jsonString, 'utf8');
      console.log(`Results saved successfully to: ${outputPath}`);
      
      // Verify file was created and check its size
      try {
        const stats = await fs.stat(outputPath);
        console.log(`File size: ${stats.size} bytes`);
        
        // Try to read back the file to verify it's valid
        const readBack = await fs.readFile(outputPath, 'utf8');
        const parsed = JSON.parse(readBack);
        console.log(`Verification: File contains ${parsed.results.length} results`);
        
      } catch (verifyError) {
        console.warn('Warning: File verification failed but file was saved:', verifyError.message);
      }
      
    } catch (writeError) {
      console.warn('Warning: Main file save failed, trying fallback methods:', writeError.message);
      
      // Try saving a smaller test file first
      try {
        const testData = { test: 'data', timestamp: new Date().toISOString() };
        const testPath = path.join(__dirname, 'test_write.json');
        await fs.writeFile(testPath, JSON.stringify(testData, null, 2), 'utf8');
        console.log('Test file write successful');
        
        // Now try saving just the summary
        const summaryOnlyPath = path.join(__dirname, 'assembly_bills_summary.json');
        await fs.writeFile(summaryOnlyPath, JSON.stringify(compiledData.summary, null, 2), 'utf8');
        console.log(`Summary saved to: ${summaryOnlyPath}`);
        
        // Try saving results in chunks
        const chunkSize = 100;
        for (let i = 0; i < uniqueResults.length; i += chunkSize) {
          const chunk = uniqueResults.slice(i, i + chunkSize);
          const chunkPath = path.join(__dirname, `assembly_bills_chunk_${Math.floor(i / chunkSize) + 1}.json`);
          await fs.writeFile(chunkPath, JSON.stringify({ chunk_number: Math.floor(i / chunkSize) + 1, data: chunk }, null, 2), 'utf8');
          console.log(`Chunk ${Math.floor(i / chunkSize) + 1} saved to: ${chunkPath}`);
        }
        
      } catch (fallbackError) {
        console.warn('Warning: Even fallback saves failed, but processing continues:', fallbackError.message);
        // Don't throw - just continue with the process
      }
    }
    
    // Clean up temp file
    try {
      await fs.unlink(path.join(__dirname, 'assembly_bills_api_results_temp.json'));
      console.log('Temporary file cleaned up');
    } catch (cleanupError) {
      // Ignore cleanup errors
      console.log('Note: Temp file cleanup skipped (file may not exist)');
    }
    
    console.log(`\nAPI calls completed!`);
    console.log(`Total processed: ${uniqueResults.length}`);
    console.log(`Successful: ${compiledData.summary.successful_calls}`);
    console.log(`Failed: ${compiledData.summary.failed_calls}`);
    console.log(`No data: ${compiledData.summary.no_data_calls}`);
    if (compiledData.summary.duplicates_removed > 0) {
      console.log(`Duplicates removed: ${compiledData.summary.duplicates_removed}`);
    }
    
    return compiledData;
  } catch (error) {
    console.error('Error in loadFilteredBillsAndCallAPI:', error);
    throw error;
  }
}

// Run the script if this file is executed directly
if (require.main === module) {
  const args = process.argv.slice(2);
  
  if (args.includes('--filter-only')) {
    // Run original processing only
    processBillsData()
      .then(results => {
        console.log(`\nSummary: Processed ${results.length} unique bills that were passed.`);
        console.log(`To call APIs for these bills, run without --filter-only flag`);
      })
      .catch(error => {
        console.error('Script failed:', error);
        process.exit(1);
      });
  } else if (args.includes('--api-only')) {
    loadFilteredBillsAndCallAPI()
      .then(apiResults => {
        console.log(`\nAll processing completed successfully!`);
        console.log(`Final Summary: ${apiResults.summary.total_bills_processed} bills processed, ${apiResults.summary.successful_calls} successful API calls`);
      })
      .catch(error => {
        console.error('Processing failed:', error);
        process.exit(1);
      });
  } else {
    // Run both filtering and API calls by default
    console.log('Step 1: Filtering bills data...');
    processBillsData()
      .then(results => {
        console.log(`\nFiltering completed: Processed ${results.length} unique bills that were passed.`);
        console.log('\nStep 2: Starting API calls for filtered bills...\n');
        return loadFilteredBillsAndCallAPI();
      })
      .then(apiResults => {
        console.log(`\nAll processing completed successfully!`);
        console.log(`Final Summary: ${apiResults.summary.total_bills_processed} bills processed, ${apiResults.summary.successful_calls} successful API calls`);
      })
      .catch(error => {
        console.error('Processing failed:', error);
        process.exit(1);
      });
  }
}

module.exports = { processBillsData, loadFilteredBillsAndCallAPI };