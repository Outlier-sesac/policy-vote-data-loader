const fs = require('fs').promises;
const path = require('path');

/**
 * Test function to rename a single PDF file
 * @param {string} originalFilename - Original filename to test
 * @returns {string} - New filename after processing
 */
function testRename(originalFilename) {
  console.log(`Testing rename for: ${originalFilename}`);
  
  // Find the 6th underscore
  let underscoreCount = 0;
  let cutIndex = -1;
  
  for (let i = 0; i < originalFilename.length; i++) {
    if (originalFilename[i] === '_') {
      underscoreCount++;
      if (underscoreCount === 6) {
        cutIndex = i;
        break;
      }
    }
  }
  
  if (cutIndex === -1) {
    console.log(`No 6th underscore found in: ${originalFilename}`);
    return originalFilename; // Return original if less than 6 underscores
  }
  
  // Extract everything before the 6th underscore and add .pdf extension
  const baseName = originalFilename.substring(0, cutIndex);
  const newFilename = baseName + '.pdf';
  
  console.log(`Original: ${originalFilename}`);
  console.log(`New:      ${newFilename}`);
  console.log(`Cut at position: ${cutIndex} (6th underscore)`);
  
  return newFilename;
}

/**
 * Generate new filename by removing text after 6th underscore
 * @param {string} filename - Original filename
 * @returns {string} - New filename
 */
function generateNewFilename(filename) {
  // Find the 6th underscore
  let underscoreCount = 0;
  let cutIndex = -1;
  
  for (let i = 0; i < filename.length; i++) {
    if (filename[i] === '_') {
      underscoreCount++;
      if (underscoreCount === 6) {
        cutIndex = i;
        break;
      }
    }
  }
  
  if (cutIndex === -1) {
    return filename; // Return original if less than 6 underscores
  }
  
  // Extract everything before the 6th underscore and add .pdf extension
  const baseName = filename.substring(0, cutIndex);
  return baseName + '.pdf';
}

/**
 * Recursively find all PDF files in a directory
 * @param {string} dirPath - Directory path to search
 * @returns {Array} - Array of file objects with full path and relative path
 */
async function findAllPdfFiles(dirPath) {
  const pdfFiles = [];
  
  async function searchDirectory(currentPath, relativePath = '') {
    try {
      const items = await fs.readdir(currentPath, { withFileTypes: true });
      
      for (const item of items) {
        const fullPath = path.join(currentPath, item.name);
        const relPath = path.join(relativePath, item.name);
        
        if (item.isDirectory()) {
          // Recursively search subdirectories
          await searchDirectory(fullPath, relPath);
        } else if (item.isFile() && item.name.toLowerCase().endsWith('.pdf')) {
          pdfFiles.push({
            fullPath: fullPath,
            relativePath: relPath,
            filename: item.name,
            directory: currentPath
          });
        }
      }
    } catch (error) {
      console.error(`Error reading directory ${currentPath}:`, error.message);
    }
  }
  
  await searchDirectory(dirPath);
  return pdfFiles;
}

/**
 * Rename all PDF files in the pdf_downloads folder
 * @param {boolean} dryRun - If true, only show what would be renamed without actually renaming
 */
async function renameAllPdfFiles(dryRun = true) {
  try {
    console.log('Starting PDF file renaming process...');
    console.log(`Mode: ${dryRun ? 'DRY RUN (no actual renaming)' : 'ACTUAL RENAMING'}`);
    
    const downloadsDir = path.join(__dirname, 'pdf_downloads');
    
    // Check if pdf_downloads directory exists
    try {
      await fs.access(downloadsDir);
    } catch (error) {
      console.error('pdf_downloads directory not found!');
      return;
    }
    
    console.log(`Searching for PDF files in: ${downloadsDir}`);
    
    // Find all PDF files
    const pdfFiles = await findAllPdfFiles(downloadsDir);
    console.log(`Found ${pdfFiles.length} PDF files to process`);
    
    if (pdfFiles.length === 0) {
      console.log('No PDF files found to rename');
      return;
    }
    
    let renamedCount = 0;
    let skippedCount = 0;
    let errorCount = 0;
    const results = [];
    
    for (const file of pdfFiles) {
      try {
        const newFilename = generateNewFilename(file.filename);
        
        if (newFilename === file.filename) {
          console.log(`⏭️  Skipping (no change needed): ${file.relativePath}`);
          skippedCount++;
          results.push({
            original: file.relativePath,
            new: file.relativePath,
            status: 'skipped',
            reason: 'No 6th underscore found or no change needed'
          });
          continue;
        }
        
        const newFullPath = path.join(file.directory, newFilename);
        
        // Check if target file already exists
        try {
          await fs.access(newFullPath);
          console.log(`⚠️  Warning: Target file already exists, skipping: ${newFilename}`);
          skippedCount++;
          results.push({
            original: file.relativePath,
            new: path.join(path.dirname(file.relativePath), newFilename),
            status: 'skipped',
            reason: 'Target file already exists'
          });
          continue;
        } catch (error) {
          // Target file doesn't exist, safe to rename
        }
        
        if (!dryRun) {
          // Actually rename the file
          await fs.rename(file.fullPath, newFullPath);
          console.log(`✅ Renamed: ${file.relativePath} → ${path.join(path.dirname(file.relativePath), newFilename)}`);
        } else {
          console.log(`🔍 Would rename: ${file.relativePath} → ${path.join(path.dirname(file.relativePath), newFilename)}`);
        }
        
        renamedCount++;
        results.push({
          original: file.relativePath,
          new: path.join(path.dirname(file.relativePath), newFilename),
          status: dryRun ? 'would_rename' : 'renamed',
          reason: 'Successfully processed'
        });
        
      } catch (error) {
        console.error(`❌ Error processing ${file.relativePath}:`, error.message);
        errorCount++;
        results.push({
          original: file.relativePath,
          new: null,
          status: 'error',
          reason: error.message
        });
      }
    }
    
    // Save results to file
    const resultsData = {
      summary: {
        total_files: pdfFiles.length,
        renamed_count: renamedCount,
        skipped_count: skippedCount,
        error_count: errorCount,
        dry_run: dryRun,
        processed_date: new Date().toISOString()
      },
      results: results
    };
    
    const resultsPath = path.join(__dirname, `pdf_rename_results_${dryRun ? 'dryrun' : 'actual'}_${new Date().toISOString().slice(0, 10)}.json`);
    await fs.writeFile(resultsPath, JSON.stringify(resultsData, null, 2), 'utf8');
    
    console.log('\n=== Summary ===');
    console.log(`Total files processed: ${pdfFiles.length}`);
    console.log(`${dryRun ? 'Would be renamed' : 'Renamed'}: ${renamedCount}`);
    console.log(`Skipped: ${skippedCount}`);
    console.log(`Errors: ${errorCount}`);
    console.log(`Results saved to: ${resultsPath}`);
    
    if (dryRun) {
      console.log('\n💡 This was a dry run. To actually rename files, call renameAllPdfFiles(false)');
    }
    
    return resultsData;
    
  } catch (error) {
    console.error('Error in renameAllPdfFiles:', error);
    throw error;
  }
}

// Run the script if this file is executed directly
if (require.main === module) {
  const args = process.argv.slice(2);
  
  if (args.includes('--test')) {
    // Test mode - test a sample filename
    console.log('=== Testing filename transformation ===\n');
    
    const testFilenames = [
      '국회본회의 회의록_047627_제20대_제354회_제13차_20171124_34.  에너지산업클러스터의 지정 및 육성에 관한 특별법안(장병완 의원 대표발의)(장병완ㆍ주승용ㆍ전혜숙ㆍ유동수ㆍ김경진ㆍ이동섭ㆍ박준영ㆍ정인화ㆍ박지원ㆍ김수민ㆍ김동철ㆍ박주선ㆍ송기석ㆍ이찬열ㆍ신용현ㆍ노웅래ㆍ이상돈ㆍ이태규ㆍ윤호중ㆍ천정배ㆍ윤영일ㆍ이개호ㆍ인재근ㆍ이채익ㆍ홍의락ㆍ최경환(국)ㆍ김민기ㆍ김중로ㆍ백재현ㆍ조배숙ㆍ권은희ㆍ손금주 의원 발의).pdf',
    ];
    
    for (const filename of testFilenames) {
      testRename(filename);
      console.log('---\n');
    }
  } else if (args.includes('--actual')) {
    // Actual renaming
    renameAllPdfFiles(false)
      .then(results => {
        console.log('\nPDF file renaming completed!');
      })
      .catch(error => {
        console.error('PDF renaming failed:', error);
        process.exit(1);
      });
  } else {
    // Default: dry run
    renameAllPdfFiles(true)
      .then(results => {
        console.log('\nDry run completed! Use --actual flag to perform actual renaming.');
      })
      .catch(error => {
        console.error('PDF renaming dry run failed:', error);
        process.exit(1);
      });
  }
}

module.exports = { 
  testRename, 
  generateNewFilename, 
  findAllPdfFiles, 
  renameAllPdfFiles 
};
