import asyncio
import aiohttp
import json
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

class ConferencePdfDownloader:
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.downloads_dir = self.base_dir / 'pdf_downloads'

    def load_tracking_data(self) -> List[Dict[str, Any]]:
        """Load existing tracking data"""
        try:
            tracking_path = self.base_dir / 'pdf_tracking_list.json'
            with open(tracking_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                return data.get('pdfs', [])
        except:
            print('No existing tracking file found, starting fresh')
            return []

    def save_tracking_data(self, pdfs: List[Dict[str, Any]]) -> None:
        """Save tracking data to JSON file"""
        try:
            tracking_path = self.base_dir / 'pdf_tracking_list.json'
            data = {'pdfs': pdfs}
            with open(tracking_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as error:
            print(f'Warning: Failed to save tracking data: {error}')

    def create_pdf_tracking_entry(self, bill_id: str, bill_name: str, conference_kind: str,
                                conference_id: str, eraco: str, session: str, degree: str,
                                conference_date: str, download_url: str, filename: str,
                                full_path: str, relative_path: str, file_exists: bool) -> Dict[str, Any]:
        """Create a PDF tracking entry"""
        return {
            'bill_id': bill_id,
            'bill_name': bill_name,
            'conference_kind': conference_kind,
            'conference_id': conference_id,
            'eraco': eraco,
            'session': session,
            'degree': degree,
            'conference_date': conference_date,
            'download_url': download_url,
            'filename': filename,
            'full_path': full_path,
            'relative_path': relative_path,
            'file_exists': file_exists,
            'tracked_date': datetime.now().isoformat()
        }

    def is_pdf_already_tracked(self, tracking_list: List[Dict[str, Any]], bill_id: str,
                             conference_id: str, bill_name: str, download_url: str) -> bool:
        """Check if PDF is already tracked"""
        return any(
            entry.get('bill_id') == bill_id and
            entry.get('conference_id') == conference_id and
            entry.get('bill_name') == bill_name and
            entry.get('download_url') == download_url
            for entry in tracking_list
        )

    def sanitize_filename(self, text: str) -> str:
        """Sanitize text for use in filename"""
        if not text:
            return '_'
        # Remove invalid filename characters
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', str(text))
        return sanitized.strip() or '_'

    async def download_pdf(self, session: aiohttp.ClientSession, url: str, file_path: Path) -> bool:
        """Download a single PDF file"""
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            
            async with session.get(url, headers=headers, timeout=30) as response:
                response.raise_for_status()
                
                # Save PDF file
                with open(file_path, 'wb') as f:
                    async for chunk in response.content.iter_chunked(8192):
                        f.write(chunk)
                
                return True
                
        except Exception as error:
            print(f'Download error: {error}')
            return False

    async def download_conference_pdfs(self) -> None:
        """Main method to download conference PDFs"""
        try:
            print('Starting PDF download process...')
            
            # Load existing tracking data
            existing_tracking = self.load_tracking_data()
            print(f'Found {len(existing_tracking)} existing tracking entries')
            
            # Read the conference API results file
            file_path = self.base_dir / 'assembly_bills_conference_api_results.json'
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            if 'results' not in data or not isinstance(data['results'], list):
                raise Exception('No results array found in the file')
            
            print(f'Found {len(data["results"])} total results')
            
            # Filter results that have api_response with data
            valid_results = []
            for result in data['results']:
                try:
                    if (result.get('api_response') and 
                        isinstance(result['api_response'], list) and 
                        len(result['api_response']) >= 2 and
                        result['api_response'][1] and
                        'row' in result['api_response'][1] and
                        isinstance(result['api_response'][1]['row'], list)):
                        valid_results.append(result)
                except:
                    continue
            
            print(f'Found {len(valid_results)} results with valid API responses')
            
            # Create downloads directory
            self.downloads_dir.mkdir(exist_ok=True)
            
            total_downloaded = 0
            total_skipped = 0
            total_errors = 0
            new_tracking_entries = existing_tracking.copy()
            
            connector = aiohttp.TCPConnector(limit=5)
            timeout = aiohttp.ClientTimeout(total=30)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                for result in valid_results:
                    try:
                        rows = result['api_response'][1]['row']
                        
                        if not rows:
                            print('No conference data found for this result')
                            total_skipped += 1
                            continue
                        
                        # Get BILL_ID from the first row (all rows should have the same BILL_ID)
                        bill_id = rows[0].get('BILL_ID')
                        
                        if not bill_id:
                            print('No BILL_ID found in response')
                            total_skipped += 1
                            continue
                        
                        print(f'Processing BILL_ID: {bill_id}')
                        
                        # Create directory for this BILL_ID
                        bill_dir = self.downloads_dir / bill_id
                        bill_dir.mkdir(exist_ok=True)
                        
                        # Download PDFs for each row
                        for i, row in enumerate(rows):
                            if not row.get('DOWN_URL'):
                                print(f'  No download URL for row {i + 1}')
                                continue
                            
                            try:
                                # Create filename from conference data fields
                                conf_knd = self.sanitize_filename(row.get('CONF_KND', '_'))
                                conf_id = self.sanitize_filename(row.get('CONF_ID', '_'))
                                eraco = self.sanitize_filename(row.get('ERACO', '_'))
                                sess = self.sanitize_filename(row.get('SESS', '_'))
                                dgr = self.sanitize_filename(row.get('DGR', '_'))
                                conf_dt = self.sanitize_filename(row.get('CONF_DT', '_'))
                                
                                filename = f'{conf_knd}_{conf_id}_{eraco}_{sess}_{dgr}_{conf_dt}.pdf'
                                file_path = bill_dir / filename
                                relative_path = Path('pdf_downloads') / bill_id / filename
                                
                                # Check if this PDF is already tracked
                                if self.is_pdf_already_tracked(new_tracking_entries, bill_id, 
                                                             conf_id, row.get('BILL_NM', ''), row['DOWN_URL']):
                                    print(f'  ○ Already tracked: {filename}')
                                    total_skipped += 1
                                    continue
                                
                                # Check if file already exists on filesystem
                                file_exists = file_path.exists()
                                if file_exists:
                                    print(f'  ✓ File already exists: {filename}')
                                    total_skipped += 1
                                else:
                                    print(f'  Downloading: {filename}')
                                    
                                    # Download PDF
                                    success = await self.download_pdf(session, row['DOWN_URL'], file_path)
                                    
                                    if success:
                                        print(f'  ✓ Downloaded: {filename}')
                                        total_downloaded += 1
                                        file_exists = True
                                        
                                        # Add small delay between downloads
                                        await asyncio.sleep(1)
                                    else:
                                        print(f'  ✗ Download failed: {filename}')
                                        total_errors += 1
                                
                                # Add to tracking list (even if file already existed)
                                tracking_entry = self.create_pdf_tracking_entry(
                                    bill_id,
                                    row.get('BILL_NM', '_'),
                                    conf_knd,
                                    conf_id,
                                    eraco,
                                    sess,
                                    dgr,
                                    conf_dt,
                                    row['DOWN_URL'],
                                    filename if file_exists else 'DOWNLOAD_FAILED',
                                    str(file_path) if file_exists else 'DOWNLOAD_FAILED',
                                    str(relative_path) if file_exists else 'DOWNLOAD_FAILED',
                                    file_exists
                                )
                                
                                new_tracking_entries.append(tracking_entry)
                                
                                # Save tracking data periodically (every 10 downloads)
                                if len(new_tracking_entries) % 10 == 0:
                                    self.save_tracking_data(new_tracking_entries)
                                
                            except Exception as download_error:
                                print(f'  ✗ Error downloading {row["DOWN_URL"]}: {download_error}')
                                total_errors += 1
                                
                                # Still add to tracking list even if download failed
                                tracking_entry = self.create_pdf_tracking_entry(
                                    bill_id,
                                    row.get('BILL_NM', '_'),
                                    row.get('CONF_KND', '_'),
                                    row.get('CONF_ID', '_'),
                                    row.get('ERACO', '_'),
                                    row.get('SESS', '_'),
                                    row.get('DGR', '_'),
                                    row.get('CONF_DT', '_'),
                                    row['DOWN_URL'],
                                    'DOWNLOAD_FAILED',
                                    'DOWNLOAD_FAILED',
                                    'DOWNLOAD_FAILED',
                                    False
                                )
                                
                                new_tracking_entries.append(tracking_entry)
                        
                    except Exception as result_error:
                        print(f'Error processing result: {result_error}')
                        total_errors += 1
            
            # Save final tracking data
            self.save_tracking_data(new_tracking_entries)
            
            print('\n=== Download Summary ===')
            print(f'Total PDFs downloaded: {total_downloaded}')
            print(f'Total skipped: {total_skipped}')
            print(f'Total errors: {total_errors}')
            print(f'Total tracked entries: {len(new_tracking_entries)}')
            print(f'Downloads saved to: {self.downloads_dir}')
            print(f'Tracking data saved to: pdf_tracking_list.json')
            
        except Exception as error:
            print(f'Error in download_conference_pdfs: {error}')
            raise error

def main():
    """Main function to run the PDF downloader"""
    async def run():
        downloader = ConferencePdfDownloader()
        await downloader.download_conference_pdfs()
        print('PDF download process completed successfully!')
    
    asyncio.run(run())

if __name__ == "__main__":
    main()