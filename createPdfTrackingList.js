const fs = require('fs').promises;
const path = require('path');

async function trackConfPdfs() {
  try {
    console.log('Starting PDF tracking process...');
    
    // Read the conference API results file
    const filePath = path.join(__dirname, 'assembly_bills_conference_api_results.json');
    const fileContent = await fs.readFile(filePath, 'utf8');
    const data = JSON.parse(fileContent);
    
    if (!data.results || !Array.isArray(data.results)) {
      throw new Error('No results array found in the file');
    }
    
    console.log(`Found ${data.results.length} total results`);
    
    // Filter results that have api_response with data
    const validResults = data.results.filter(result => 
      result.api_response && 
      Array.isArray(result.api_response) && 
      result.api_response.length >= 2 &&
      result.api_response[1] &&
      result.api_response[1].row &&
      Array.isArray(result.api_response[1].row)
    );
    
    console.log(`Found ${validResults.length} results with valid API responses`);
    
    // Create downloads directory path (for reference)
    const downloadsDir = path.join(__dirname, 'pdf_downloads');
    
    let totalTracked = 0;
    let totalSkipped = 0;
    const pdfTrackingData = [];
    
    for (const result of validResults) {
      try {
        const rows = result.api_response[1].row;
        
        if (rows.length === 0) {
          console.log(`No conference data found for this result`);
          totalSkipped++;
          continue;
        }
        
        // Get BILL_ID from the first row (all rows should have the same BILL_ID)
        const billId = rows[0].BILL_ID;
        
        if (!billId) {
          console.log('No BILL_ID found in response');
          totalSkipped++;
          continue;
        }
        
        console.log(`Processing BILL_ID: ${billId}`);
        
        // Define directory for this BILL_ID
        const billDir = path.join(downloadsDir, billId);
        
        // Track PDFs for each row
        for (let i = 0; i < rows.length; i++) {
          const row = rows[i];
          
          if (!row.DOWN_URL) {
            console.log(`  No download URL for row ${i + 1}`);
            continue;
          }
          
          try {
            // Create filename from all conference data fields
            const confKnd = (row.CONF_KND || '_').replace(/[<>:"/\\|?*]/g, '_');
            const confId = (row.CONF_ID || '_').replace(/[<>:"/\\|?*]/g, '_');
            const eraco = (row.ERACO || '_').replace(/[<>:"/\\|?*]/g, '_');
            const sess = (row.SESS || '_').replace(/[<>:"/\\|?*]/g, '_');
            const dgr = (row.DGR || '_').replace(/[<>:"/\\|?*]/g, '_');
            const confDt = (row.CONF_DT || '_').replace(/[<>:"/\\|?*]/g, '_').trim();
            
            const filename = `${confKnd}_${confId}_${eraco}_${sess}_${dgr}_${confDt}.pdf`;
            const fullFilePath = path.join(billDir, filename);
            
            // Check if file already exists
            let fileExists = false;
            try {
              await fs.access(fullFilePath);
              fileExists = true;
              console.log(`  ✓ File already exists: ${filename}`);
            } catch (error) {
              // File doesn't exist
              console.log(`  ○ File to be downloaded: ${filename}`);
            }
            
            // Track this PDF
            pdfTrackingData.push({
              bill_id: billId,
              bill_name: row.BILL_NM,
              conference_kind: row.CONF_KND,
              conference_id: row.CONF_ID,
              eraco: row.ERACO,
              session: row.SESS,
              degree: row.DGR,
              conference_date: row.CONF_DT,
              download_url: row.DOWN_URL,
              filename: filename,
              full_path: fullFilePath,
              relative_path: path.join('pdf_downloads', billId, filename),
              file_exists: fileExists,
              tracked_date: new Date().toISOString()
            });
            
            totalTracked++;
            
          } catch (trackingError) {
            console.error(`  ✗ Error tracking ${row.DOWN_URL}:`, trackingError.message);
          }
        }
        
      } catch (resultError) {
        console.error(`Error processing result:`, resultError.message);
      }
    }
    
    // Save tracking data to JSON file
    const trackingData = {
      summary: {
        total_pdfs_tracked: totalTracked,
        total_existing_files: pdfTrackingData.filter(p => p.file_exists).length,
        total_to_download: pdfTrackingData.filter(p => !p.file_exists).length,
        total_skipped: totalSkipped,
        generated_date: new Date().toISOString()
      },
      pdfs: pdfTrackingData
    };
    
    const trackingFilePath = path.join(__dirname, 'pdf_tracking_list.json');
    await fs.writeFile(trackingFilePath, JSON.stringify(trackingData, null, 2), 'utf8');
    
    console.log('\n=== PDF Tracking Summary ===');
    console.log(`Total PDFs tracked: ${totalTracked}`);
    console.log(`Files already exist: ${trackingData.summary.total_existing_files}`);
    console.log(`Files to download: ${trackingData.summary.total_to_download}`);
    console.log(`Total skipped: ${totalSkipped}`);
    console.log(`Tracking data saved to: ${trackingFilePath}`);
    
  } catch (error) {
    console.error('Error in trackConfPdfs:', error);
    throw error;
  }
}

// Run the script if this file is executed directly
if (require.main === module) {
  trackConfPdfs()
    .then(() => {
      console.log('PDF tracking process completed successfully!');
    })
    .catch(error => {
      console.error('PDF tracking process failed:', error);
      process.exit(1);
    });
}

module.exports = { trackConfPdfs };
