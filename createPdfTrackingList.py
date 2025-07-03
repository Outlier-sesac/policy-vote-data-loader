import json
import re
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

class PdfTracker:
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.downloads_dir = self.base_dir / 'pdf_downloads'

    def sanitize_filename(self, text: str) -> str:
        """Sanitize text for use in filename"""
        if not text:
            return '_'
        # Remove invalid filename characters
        sanitized = re.sub(r'[<>:"/\\|?*]', '_', str(text))
        return sanitized.strip() or '_'

    def track_conference_pdfs(self) -> None:
        """Create PDF tracking list without downloading"""
        try:
            print('Starting PDF tracking process...')
            
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
            
            total_tracked = 0
            total_skipped = 0
            pdf_tracking_data = []
            
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
                    
                    # Define directory for this BILL_ID
                    bill_dir = self.downloads_dir / bill_id
                    
                    # Track PDFs for each row
                    for i, row in enumerate(rows):
                        if not row.get('DOWN_URL'):
                            print(f'  No download URL for row {i + 1}')
                            continue
                        
                        try:
                            # Create filename from all conference data fields
                            conf_knd = self.sanitize_filename(row.get('CONF_KND', '_'))
                            conf_id = self.sanitize_filename(row.get('CONF_ID', '_'))
                            eraco = self.sanitize_filename(row.get('ERACO', '_'))
                            sess = self.sanitize_filename(row.get('SESS', '_'))
                            dgr = self.sanitize_filename(row.get('DGR', '_'))
                            conf_dt = self.sanitize_filename(row.get('CONF_DT', '_'))
                            
                            filename = f'{conf_knd}_{conf_id}_{eraco}_{sess}_{dgr}_{conf_dt}.pdf'
                            full_file_path = bill_dir / filename
                            
                            # Check if file already exists
                            file_exists = full_file_path.exists()
                            if file_exists:
                                print(f'  ✓ File already exists: {filename}')
                            else:
                                print(f'  ○ File to be downloaded: {filename}')
                            
                            # Track this PDF
                            pdf_tracking_data.append({
                                'bill_id': bill_id,
                                'bill_name': row.get('BILL_NM'),
                                'conference_kind': row.get('CONF_KND'),
                                'conference_id': row.get('CONF_ID'),
                                'eraco': row.get('ERACO'),
                                'session': row.get('SESS'),
                                'degree': row.get('DGR'),
                                'conference_date': row.get('CONF_DT'),
                                'download_url': row['DOWN_URL'],
                                'filename': filename,
                                'full_path': str(full_file_path),
                                'relative_path': str(Path('pdf_downloads') / bill_id / filename),
                                'file_exists': file_exists,
                                'tracked_date': datetime.now().isoformat()
                            })
                            
                            total_tracked += 1
                            
                        except Exception as tracking_error:
                            print(f'  ✗ Error tracking {row["DOWN_URL"]}: {tracking_error}')
                    
                except Exception as result_error:
                    print(f'Error processing result: {result_error}')
            
            # Save tracking data to JSON file
            tracking_data = {
                'summary': {
                    'total_pdfs_tracked': total_tracked,
                    'total_existing_files': sum(1 for p in pdf_tracking_data if p['file_exists']),
                    'total_to_download': sum(1 for p in pdf_tracking_data if not p['file_exists']),
                    'total_skipped': total_skipped,
                    'generated_date': datetime.now().isoformat()
                },
                'pdfs': pdf_tracking_data
            }
            
            tracking_file_path = self.base_dir / 'pdf_tracking_list.json'
            with open(tracking_file_path, 'w', encoding='utf-8') as f:
                json.dump(tracking_data, f, ensure_ascii=False, indent=2)
            
            print('\n=== PDF Tracking Summary ===')
            print(f'Total PDFs tracked: {total_tracked}')
            print(f'Files already exist: {tracking_data["summary"]["total_existing_files"]}')
            print(f'Files to download: {tracking_data["summary"]["total_to_download"]}')
            print(f'Total skipped: {total_skipped}')
            print(f'Tracking data saved to: {tracking_file_path}')
            
        except Exception as error:
            print(f'Error in track_conference_pdfs: {error}')
            raise error

def main():
    """Main function to run the PDF tracker"""
    tracker = PdfTracker()
    tracker.track_conference_pdfs()
    print('PDF tracking process completed successfully!')

if __name__ == "__main__":
    main()