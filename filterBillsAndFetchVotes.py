import asyncio
import aiohttp
import json
import time
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
import argparse
from dotenv import load_dotenv

load_dotenv()

class BillsFilterAndVoteFetcher:
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.api_key = os.getenv('API_KEY')
        if not self.api_key:
            raise ValueError('API_KEY is not set in environment variables. Please check your .env file.')
        self.base_url = 'https://open.assembly.go.kr/portal/openapi/nojepdqqaweusdfbi'

    def process_bills_data(self) -> List[Dict[str, Any]]:
        """Process assembly bills data and filter passed bills"""
        try:
            print('Starting to process assembly bills data...')
            
            # Get all files in the current directory
            files = [f for f in self.base_dir.iterdir() if f.is_file()]
            
            # Filter files that start with 'assembly_bills_age' and end with '.json'
            bills_files = [f for f in files if f.name.startswith('assembly_bills_age') and f.name.endswith('.json')]
            
            print(f'Found {len(bills_files)} assembly bills files to process.')
            
            filtered_results = []
            
            for file_path in bills_files:
                print(f'Processing file: {file_path.name}')
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        json_data = json.load(f)
                    
                    if 'data' not in json_data or not isinstance(json_data['data'], list):
                        print(f'Skipping {file_path.name} - no data array found')
                        continue
                    
                    # Filter data where PROC_RESULT is '원안가결' or '수정가결'
                    filtered = [
                        item for item in json_data['data']
                        if (item.get('PROC_RESULT') in ['원안가결', '수정가결'] and 
                            int(item.get('AGE', 0)) >= 17)
                    ]
                    
                    # Extract BILL_ID and AGE
                    extracted = [
                        {
                            'BILL_ID': item['BILL_ID'],
                            'AGE': item['AGE']
                        }
                        for item in filtered
                    ]
                    
                    filtered_results.extend(extracted)
                    
                    print(f'Found {len(filtered)} matching records in {file_path.name}')
                    
                except Exception as error:
                    print(f'Error processing file {file_path.name}: {error}')
            
            # Remove duplicates based on BILL_ID
            unique_results = []
            seen_bill_ids = set()
            
            for item in filtered_results:
                if item['BILL_ID'] not in seen_bill_ids:
                    seen_bill_ids.add(item['BILL_ID'])
                    unique_results.append(item)
            
            print(f'Total filtered records: {len(filtered_results)}')
            print(f'Unique records after deduplication: {len(unique_results)}')
            
            # Save results to JSON file
            output_data = {
                'total_count': len(unique_results),
                'filtered_date': datetime.now().isoformat(),
                'filter_criteria': "PROC_RESULT = '원안가결' OR '수정가결'",
                'data': unique_results
            }
            
            output_path = self.base_dir / 'assembly_filtered_bills_passed.json'
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            
            print(f'Results saved to: {output_path}')
            print('Processing completed successfully!')
            
            return unique_results
            
        except Exception as error:
            print(f'Error in process_bills_data: {error}')
            raise error

    def load_filtered_bills(self) -> Dict[str, Any]:
        """Load filtered bills data from JSON file"""
        try:
            # Try main filtered file first
            filtered_path = self.base_dir / 'assembly_filtered_bills_passed.json'
            if filtered_path.exists():
                with open(filtered_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            
            # Fallback to alternative filename
            filtered_path = self.base_dir / 'assembly_filtered_passed_bills.json'
            with open(filtered_path, 'r', encoding='utf-8') as f:
                return json.load(f)
                
        except Exception as error:
            raise Exception(f'Could not load filtered bills file: {error}')

    def load_existing_results(self) -> List[Dict[str, Any]]:
        """Load existing API results to avoid duplicates"""
        try:
            existing_path = self.base_dir / 'assembly_bills_api_results.json'
            with open(existing_path, 'r', encoding='utf-8') as f:
                existing_data = json.load(f)
                return existing_data.get('results', [])
        except:
            print('No existing API results found, starting fresh')
            return []

    async def call_vote_api(self, session: aiohttp.ClientSession, bill: Dict[str, Any]) -> Dict[str, Any]:
        """Call API for a single bill to get vote data"""
        try:
            url = f'{self.base_url}?KEY={self.api_key}&Type=json&pIndex=1&pSize=1000&BILL_ID={bill["BILL_ID"]}&AGE={bill["AGE"]}'
            
            print(f'Calling API for BILL_ID: {bill["BILL_ID"]}, AGE: {bill["AGE"]}')
            
            async with session.get(url, timeout=30) as response:
                response.raise_for_status()
                data = await response.json()
                
                if data and 'nojepdqqaweusdfbi' in data:
                    print(f'✓ Success for BILL_ID: {bill["BILL_ID"]}')
                    
                    return {
                        'BILL_ID': bill['BILL_ID'],
                        'AGE': bill['AGE'],
                        'api_response': data['nojepdqqaweusdfbi'],
                        'status': 'success',
                        'timestamp': datetime.now().isoformat()
                    }
                else:
                    print(f'- No data for BILL_ID: {bill["BILL_ID"]}')
                    return {
                        'BILL_ID': bill['BILL_ID'],
                        'AGE': bill['AGE'],
                        'api_response': None,
                        'status': 'no_data',
                        'timestamp': datetime.now().isoformat()
                    }
                    
        except Exception as error:
            print(f'✗ Error for BILL_ID {bill["BILL_ID"]}: {error}')
            return {
                'BILL_ID': bill['BILL_ID'],
                'AGE': bill['AGE'],
                'api_response': None,
                'status': 'error',
                'error': str(error),
                'timestamp': datetime.now().isoformat()
            }

    async def fetch_vote_data(self) -> Dict[str, Any]:
        """Load filtered bills and call API for vote data"""
        try:
            print('Loading filtered bills data...')
            
            filtered_data = self.load_filtered_bills()
            
            if 'data' not in filtered_data or not isinstance(filtered_data['data'], list):
                raise Exception('No data array found in filtered bills file')
            
            print(f'Found {len(filtered_data["data"])} bills to process')
            
            # Check for existing API results to avoid duplicates
            existing_results = self.load_existing_results()
            print(f'Found {len(existing_results)} existing API results')
            
            # Filter out bills that already have API results
            bills_to_process = []
            for bill in filtered_data['data']:
                exists = any(
                    result.get('BILL_ID') == bill['BILL_ID'] and 
                    result.get('AGE') == bill['AGE']
                    for result in existing_results
                )
                if not exists:
                    bills_to_process.append(bill)
            
            print(f'After removing duplicates: {len(bills_to_process)} bills need API calls')
            
            if not bills_to_process:
                print('All bills already have API results. No new calls needed.')
                return {
                    'summary': {
                        'total_bills_processed': len(existing_results),
                        'successful_calls': sum(1 for r in existing_results if r.get('status') == 'success'),
                        'failed_calls': sum(1 for r in existing_results if r.get('status') == 'error'),
                        'no_data_calls': sum(1 for r in existing_results if r.get('status') == 'no_data'),
                        'processed_date': datetime.now().isoformat(),
                        'note': 'No new API calls made - all bills already processed'
                    },
                    'results': existing_results
                }
            
            api_results = existing_results.copy()  # Start with existing results
            
            # Process bills in batches
            batch_size = 10
            connector = aiohttp.TCPConnector(limit=batch_size)
            timeout = aiohttp.ClientTimeout(total=30)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                for i in range(0, len(bills_to_process), batch_size):
                    batch = bills_to_process[i:i + batch_size]
                    
                    print(f'Processing batch {i // batch_size + 1}/{(len(bills_to_process) + batch_size - 1) // batch_size} ({len(batch)} bills)')
                    
                    # Process batch concurrently
                    batch_tasks = [self.call_vote_api(session, bill) for bill in batch]
                    batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                    
                    # Handle results and exceptions
                    for result in batch_results:
                        if isinstance(result, Exception):
                            print(f'Batch processing error: {result}')
                        else:
                            api_results.append(result)
                    
                    # Save intermediate results after each batch
                    intermediate_data = {
                        'summary': {
                            'total_bills_processed': len(api_results),
                            'successful_calls': sum(1 for r in api_results if r.get('status') == 'success'),
                            'failed_calls': sum(1 for r in api_results if r.get('status') == 'error'),
                            'no_data_calls': sum(1 for r in api_results if r.get('status') == 'no_data'),
                            'processed_date': datetime.now().isoformat(),
                            'batch_completed': i // batch_size + 1
                        },
                        'results': api_results
                    }
                    
                    try:
                        intermediate_path = self.base_dir / 'assembly_bills_api_results_temp.json'
                        with open(intermediate_path, 'w', encoding='utf-8') as f:
                            json.dump(intermediate_data, f, ensure_ascii=False, indent=2)
                        print(f'Intermediate results saved ({len(api_results)} total results)')
                    except Exception as intermediate_error:
                        print(f'Warning: Failed to save intermediate results: {intermediate_error}')
                    
                    # Add delay between batches
                    if i + batch_size < len(bills_to_process):
                        print('Waiting 2 seconds before next batch...')
                        await asyncio.sleep(2)
            
            # Remove duplicates from final results
            unique_results = []
            seen_bills = set()
            
            for result in api_results:
                key = f"{result.get('BILL_ID')}_{result.get('AGE')}"
                if key not in seen_bills:
                    seen_bills.add(key)
                    unique_results.append(result)
                else:
                    print(f'Duplicate found and removed: BILL_ID {result.get("BILL_ID")}, AGE {result.get("AGE")}')
            
            # Compile final results
            compiled_data = {
                'summary': {
                    'total_bills_processed': len(unique_results),
                    'successful_calls': sum(1 for r in unique_results if r.get('status') == 'success'),
                    'failed_calls': sum(1 for r in unique_results if r.get('status') == 'error'),
                    'no_data_calls': sum(1 for r in unique_results if r.get('status') == 'no_data'),
                    'processed_date': datetime.now().isoformat(),
                    'duplicates_removed': len(api_results) - len(unique_results)
                },
                'results': unique_results
            }
            
            # Save compiled results
            self.save_results(compiled_data)
            
            print(f'\nAPI calls completed!')
            print(f'Total processed: {len(unique_results)}')
            print(f'Successful: {compiled_data["summary"]["successful_calls"]}')
            print(f'Failed: {compiled_data["summary"]["failed_calls"]}')
            print(f'No data: {compiled_data["summary"]["no_data_calls"]}')
            
            return compiled_data
            
        except Exception as error:
            print(f'Error in fetch_vote_data: {error}')
            raise error

    def save_results(self, compiled_data: Dict[str, Any]) -> None:
        """Save compiled results to file with error handling"""
        print('Saving compiled results...')
        output_path = self.base_dir / 'assembly_bills_api_results.json'
        
        try:
            # Try to save the main file
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(compiled_data, f, ensure_ascii=False, indent=2)
            print(f'Results saved successfully to: {output_path}')
            
            # Verify file was created
            if output_path.exists():
                file_size = output_path.stat().st_size
                print(f'File size: {file_size} bytes')
                
                # Try to read back the file to verify it's valid
                with open(output_path, 'r', encoding='utf-8') as f:
                    parsed = json.load(f)
                print(f'Verification: File contains {len(parsed.get("results", []))} results')
            
        except Exception as write_error:
            print(f'Warning: Main file save failed: {write_error}')
            
            # Try saving just the summary as fallback
            try:
                summary_path = self.base_dir / 'assembly_bills_summary_fallback.json'
                with open(summary_path, 'w', encoding='utf-8') as f:
                    json.dump(compiled_data['summary'], f, ensure_ascii=False, indent=2)
                print(f'Summary saved to: {summary_path}')
                
                # Try saving results in chunks
                chunk_size = 100
                results = compiled_data.get('results', [])
                for i in range(0, len(results), chunk_size):
                    chunk = results[i:i + chunk_size]
                    chunk_path = self.base_dir / f'assembly_bills_chunk_{i // chunk_size + 1}.json'
                    chunk_data = {'chunk_number': i // chunk_size + 1, 'data': chunk}
                    with open(chunk_path, 'w', encoding='utf-8') as f:
                        json.dump(chunk_data, f, ensure_ascii=False, indent=2)
                    print(f'Chunk {i // chunk_size + 1} saved to: {chunk_path}')
                
            except Exception as fallback_error:
                print(f'Warning: Even fallback saves failed: {fallback_error}')
        
        # Clean up temp file
        try:
            temp_path = self.base_dir / 'assembly_bills_api_results_temp.json'
            if temp_path.exists():
                temp_path.unlink()
                print('Temporary file cleaned up')
        except:
            print('Note: Temp file cleanup skipped (file may not exist)')

def main():
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(description='Filter assembly bills and fetch vote data')
    parser.add_argument('--filter-only', action='store_true', 
                       help='Run filtering only, skip API calls')
    parser.add_argument('--api-only', action='store_true', 
                       help='Run API calls only, skip filtering')
    
    args = parser.parse_args()
    
    fetcher = BillsFilterAndVoteFetcher()
    
    if args.filter_only:
        # Run filtering only
        results = fetcher.process_bills_data()
        print(f'\nSummary: Processed {len(results)} unique bills that were passed.')
        print('To call APIs for these bills, run without --filter-only flag')
    
    elif args.api_only:
        # Run API calls only
        async def run_api_only():
            api_results = await fetcher.fetch_vote_data()
            print(f'\nAll processing completed successfully!')
            print(f'Final Summary: {api_results["summary"]["total_bills_processed"]} bills processed, {api_results["summary"]["successful_calls"]} successful API calls')
        
        asyncio.run(run_api_only())
    
    else:
        # Run both filtering and API calls by default
        async def run_both():
            print('Step 1: Filtering bills data...')
            results = fetcher.process_bills_data()
            print(f'\nFiltering completed: Processed {len(results)} unique bills that were passed.')
            print('\nStep 2: Starting API calls for filtered bills...\n')
            
            api_results = await fetcher.fetch_vote_data()
            print(f'\nAll processing completed successfully!')
            print(f'Final Summary: {api_results["summary"]["total_bills_processed"]} bills processed, {api_results["summary"]["successful_calls"]} successful API calls')
        
        asyncio.run(run_both())

if __name__ == "__main__":
    main()