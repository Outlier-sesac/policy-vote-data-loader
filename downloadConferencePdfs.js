const fs = require('fs').promises;
const path = require('path');
const axios = require('axios');

async function loadTrackingData() {
  try {
    const trackingPath = path.join(__dirname, 'pdf_tracking_list.json');
    const content = await fs.readFile(trackingPath, 'utf8');
    const data = JSON.parse(content);
    return data.pdfs || [];
  } catch (error) {
    console.log('No existing tracking file found, starting fresh');
    return [];
  }
}

async function saveTrackingData(pdfs) {
  try {
    const trackingPath = path.join(__dirname, 'pdf_tracking_list.json');
    const data = { pdfs };
    await fs.writeFile(trackingPath, JSON.stringify(data, null, 2), 'utf8');
  } catch (error) {
    console.warn('Warning: Failed to save tracking data:', error.message);
  }
}

function createPdfTrackingEntry(billId, billName, conferenceKind, conferenceId, eraco, session, degree, conferenceDate, downloadUrl, filename, fullPath, relativePath, fileExists) {
  return {
    bill_id: billId,
    bill_name: billName,
    conference_kind: conferenceKind,
    conference_id: conferenceId,
    eraco: eraco,
    session: session,
    degree: degree,
    conference_date: conferenceDate,
    download_url: downloadUrl,
    filename: filename,
    full_path: fullPath,
    relative_path: relativePath,
    file_exists: fileExists,
    tracked_date: new Date().toISOString()
  };
}

function isPdfAlreadyTracked(trackingList, billId, conferenceId, billName, downloadUrl) {
  return trackingList.some(entry => 
    entry.bill_id === billId && 
    entry.conference_id === conferenceId && 
    entry.bill_name === billName &&
    entry.download_url === downloadUrl
  );
}

async function downloadConfPdfs() {
  try {
    console.log('Starting PDF download process...');
    
    // Load existing tracking data
    const existingTracking = await loadTrackingData();
    console.log(`Found ${existingTracking.length} existing tracking entries`);
    
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
    
    // Create downloads directory
    const downloadsDir = path.join(__dirname, 'pdf_downloads');
    try {
      await fs.mkdir(downloadsDir, { recursive: true });
    } catch (error) {
      // Directory already exists
    }
    
    let totalDownloaded = 0;
    let totalSkipped = 0;
    let totalErrors = 0;
    let newTrackingEntries = [...existingTracking];
    
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
        
        // Create directory for this BILL_ID
        const billDir = path.join(downloadsDir, billId);
        try {
          await fs.mkdir(billDir, { recursive: true });
        } catch (error) {
          // Directory already exists
        }
        
        // Download PDFs for each row
        for (let i = 0; i < rows.length; i++) {
          const row = rows[i];
          
          if (!row.DOWN_URL) {
            console.log(`  No download URL for row ${i + 1}`);
            continue;
          }
          
          try {
            // Create filename from conference data fields (excluding BILL_NM)
            const confKnd = (row.CONF_KND || '_').replace(/[<>:"/\\|?*]/g, '_');
            const confId = (row.CONF_ID || '_').replace(/[<>:"/\\|?*]/g, '_');
            const eraco = (row.ERACO || '_').replace(/[<>:"/\\|?*]/g, '_');
            const sess = (row.SESS || '_').replace(/[<>:"/\\|?*]/g, '_');
            const dgr = (row.DGR || '_').replace(/[<>:"/\\|?*]/g, '_');
            const confDt = (row.CONF_DT || '_').replace(/[<>:"/\\|?*]/g, '_').trim();
            
            const filename = `${confKnd}_${confId}_${eraco}_${sess}_${dgr}_${confDt}.pdf`;
            const filePath = path.join(billDir, filename);
            const relativePath = path.join('pdf_downloads', billId, filename);
            
            // Check if this PDF is already tracked (including download URL for precise matching)
            if (isPdfAlreadyTracked(newTrackingEntries, billId, confId, row.BILL_NM, row.DOWN_URL)) {
              console.log(`  ○ Already tracked: ${filename}`);
              totalSkipped++;
              continue;
            }
            
            // Check if file already exists on filesystem
            let fileExists = false;
            try {
              await fs.access(filePath);
              fileExists = true;
              console.log(`  ✓ File already exists: ${filename}`);
              totalSkipped++;
            } catch (error) {
              // File doesn't exist, proceed with download
              console.log(`  Downloading: ${filename}`);
              
              // Download PDF
              const response = await axios.get(row.DOWN_URL, {
                responseType: 'arraybuffer',
                timeout: 30000, // 30 second timeout
                headers: {
                  'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
                }
              });
              
              // Save PDF file
              await fs.writeFile(filePath, response.data);
              console.log(`  ✓ Downloaded: ${filename}`);
              totalDownloaded++;
              fileExists = true;
              
              // Add small delay between downloads
              await new Promise(resolve => setTimeout(resolve, 1000));
            }
            
            // Add to tracking list (even if file already existed)
            const trackingEntry = createPdfTrackingEntry(
              billId,
              row.BILL_NM || '_',
              confKnd,
              confId,
              eraco,
              sess,
              dgr,
              confDt,
              row.DOWN_URL,
              filename,
              filePath,
              relativePath,
              fileExists
            );
            
            newTrackingEntries.push(trackingEntry);
            
            // Save tracking data periodically (every 10 downloads)
            if (newTrackingEntries.length % 10 === 0) {
              await saveTrackingData(newTrackingEntries);
            }
            
          } catch (downloadError) {
            console.error(`  ✗ Error downloading ${row.DOWN_URL}:`, downloadError.message);
            totalErrors++;
            
            // Still add to tracking list even if download failed
            const trackingEntry = createPdfTrackingEntry(
              billId,
              row.BILL_NM || '_',
              row.CONF_KND || '_',
              row.CONF_ID || '_',
              row.ERACO || '_',
              row.SESS || '_',
              row.DGR || '_',
              row.CONF_DT || '_',
              row.DOWN_URL,
              'DOWNLOAD_FAILED',
              'DOWNLOAD_FAILED',
              'DOWNLOAD_FAILED',
              false
            );
            
            newTrackingEntries.push(trackingEntry);
          }
        }
        
      } catch (resultError) {
        console.error(`Error processing result:`, resultError.message);
        totalErrors++;
      }
    }
    
    // Save final tracking data
    await saveTrackingData(newTrackingEntries);
    
    console.log('\n=== Download Summary ===');
    console.log(`Total PDFs downloaded: ${totalDownloaded}`);
    console.log(`Total skipped: ${totalSkipped}`);
    console.log(`Total errors: ${totalErrors}`);
    console.log(`Total tracked entries: ${newTrackingEntries.length}`);
    console.log(`Downloads saved to: ${downloadsDir}`);
    console.log(`Tracking data saved to: pdf_tracking_list.json`);
    
  } catch (error) {
    console.error('Error in downloadConfPdfs:', error);
    throw error;
  }
}

// Run the script if this file is executed directly
if (require.main === module) {
  downloadConfPdfs()
    .then(() => {
      console.log('PDF download process completed successfully!');
    })
    .catch(error => {
      console.error('PDF download process failed:', error);
      process.exit(1);
    });
}

module.exports = { downloadConfPdfs };
module.exports = { downloadConfPdfs };
