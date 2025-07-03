import asyncio
import aiohttp
import json
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from dotenv import load_dotenv

load_dotenv()

class ConferenceDataFetcher:
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.api_key = os.getenv('API_KEY')
        if not self.api_key:
            raise ValueError('API_KEY is not set in environment variables. Please check your .env file.')
        self.base_url = 'https://open.assembly.go.kr/portal/openapi/VCONFBILLCONFLIST'

    def load_filtered_bills(self) -> Dict[str, Any]:
        """Load filtered bills data from JSON file"""
        try:
            filtered_path = self.base_dir / 'assembly_filtered_bills_passed.json'
            with open(filtered_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as error:
            raise Exception(f'Could not load filtered bills file: {error}')

    async def call_conference_api(self, session: aiohttp.ClientSession, bill: Dict[str, Any]) -> Dict[str, Any]:
        """Call conference API for a single bill"""
        try:
            url = f'{self.base_url}?KEY={self.api_key}&Type=json&pIndex=1&pSize=1000&BILL_ID={bill["BILL_ID"]}'
            
            print(f'Calling CONFERENCE API for BILL_ID: {bill["BILL_ID"]}')
            
            async with session.get(url, timeout=30) as response:
                response.raise_for_status()
                data = await response.json()
                
                if data and 'VCONFBILLCONFLIST' in data:
                    print(f'✓ Success for BILL_ID: {bill["BILL_ID"]}')
                    
                    return {
                        'BILL_ID': bill['BILL_ID'],
                        'AGE': bill['AGE'],
                        'api_response': data['VCONFBILLCONFLIST'],
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

    async def fetch_conference_data(self) -> Dict[str, Any]:
        """Load filtered bills and call conference API"""
        try:
            print('Loading filtered bills data...')
            
            filtered_data = self.load_filtered_bills()
            
            if 'data' not in filtered_data or not isinstance(filtered_data['data'], list):
                raise Exception('No data array found in filtered bills file')
            
            print(f'Found {len(filtered_data["data"])} bills to process')
            
            api_results = []
            
            # Process bills in batches to avoid overwhelming the API
            batch_size = 10
            connector = aiohttp.TCPConnector(limit=batch_size)
            timeout = aiohttp.ClientTimeout(total=30)
            
            async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
                for i in range(0, len(filtered_data['data']), batch_size):
                    batch = filtered_data['data'][i:i + batch_size]
                    
                    print(f'Processing batch {i // batch_size + 1}/{(len(filtered_data["data"]) + batch_size - 1) // batch_size}')
                    
                    # Process batch concurrently
                    batch_tasks = [self.call_conference_api(session, bill) for bill in batch]
                    batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
                    
                    # Handle results and exceptions
                    for result in batch_results:
                        if isinstance(result, Exception):
                            print(f'Batch processing error: {result}')
                        else:
                            api_results.append(result)
                    
                    # Add delay between batches to be respectful to the API
                    if i + batch_size < len(filtered_data['data']):
                        print('Waiting 2 seconds before next batch...')
                        await asyncio.sleep(2)
            
            # Compile results
            successful_results = [r for r in api_results if r.get('status') == 'success']
            failed_results = [r for r in api_results if r.get('status') == 'error']
            no_data_results = [r for r in api_results if r.get('status') == 'no_data']
            
            compiled_data = {
                'summary': {
                    'total_bills_processed': len(api_results),
                    'successful_calls': len(successful_results),
                    'failed_calls': len(failed_results),
                    'no_data_calls': len(no_data_results),
                    'processed_date': datetime.now().isoformat()
                },
                'results': api_results
            }
            
            # Save compiled results
            self.save_results(compiled_data)
            
            print(f'\nAPI calls completed!')
            print(f'Total processed: {len(api_results)}')
            print(f'Successful: {len(successful_results)}')
            print(f'Failed: {len(failed_results)}')
            print(f'No data: {len(no_data_results)}')
            
            return compiled_data
            
        except Exception as error:
            print(f'Error in fetch_conference_data: {error}')
            raise error

    def save_results(self, compiled_data: Dict[str, Any]) -> None:
        """Save compiled results to file"""
        print('Saving compiled results...')
        output_path = self.base_dir / 'assembly_bills_conference_api_results.json'
        
        try:
            json_string = json.dumps(compiled_data, ensure_ascii=False, indent=2)
            print(f'JSON string length: {len(json_string)} characters')
            
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(json_string)
            print(f'Results saved successfully to: {output_path}')
            
            # Verify file was created
            if output_path.exists():
                file_size = output_path.stat().st_size
                print(f'File size: {file_size} bytes')
            
        except Exception as write_error:
            print(f'Error saving results file: {write_error}')
            
            # Try saving in chunks if main file fails
            try:
                chunk_size = 100
                results = compiled_data.get('results', [])
                for i in range(0, len(results), chunk_size):
                    chunk = results[i:i + chunk_size]
                    chunk_path = self.base_dir / f'assembly_bills_conf_chunk_{i // chunk_size + 1}.json'
                    chunk_data = {'chunk_number': i // chunk_size + 1, 'data': chunk}
                    with open(chunk_path, 'w', encoding='utf-8') as f:
                        json.dump(chunk_data, f, ensure_ascii=False, indent=2)
                    print(f'Chunk {i // chunk_size + 1} saved to: {chunk_path}')
            except Exception as chunk_error:
                print(f'Even chunk saving failed: {chunk_error}')

def main():
    """Main function to run the conference data fetcher"""
    async def run():
        fetcher = ConferenceDataFetcher()
        results = await fetcher.fetch_conference_data()
        print(f'\nProcessing completed successfully!')
        print(f'Final Summary: {results["summary"]["total_bills_processed"]} bills processed, {results["summary"]["successful_calls"]} successful API calls')
    
    asyncio.run(run())

if __name__ == "__main__":
    main()