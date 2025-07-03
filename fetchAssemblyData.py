import requests
import json
import asyncio
import aiohttp
import time
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

load_dotenv()

class AssemblyDataFetcher:
    def __init__(self):
        self.api_key = os.getenv('API_KEY')
        if not self.api_key:
            raise ValueError('API_KEY is not set in environment variables. Please check your .env file.')
        self.page_size = 1000
        self.base_dir = Path(__file__).parent
        
        self.apis = [
            {
                'name': 'ALLNAMEMBER',
                'url': 'https://open.assembly.go.kr/portal/openapi/ALLNAMEMBER',
                'filename': 'assembly_members_integrated.json'
            },
            {
                'name': 'nprlapfmaufmqytet',
                'url': 'https://open.assembly.go.kr/portal/openapi/nprlapfmaufmqytet',
                'filename': 'assembly_members_history_daesu_{DAESU}.json',
                'is_daesu_iteration': True
            },
            {
                'name': 'nwvrqwxyaytdsfvhu',
                'url': 'https://open.assembly.go.kr/portal/openapi/nwvrqwxyaytdsfvhu',
                'filename': 'assembly_members_profile.json'
            },
            {
                'name': 'nzmimeepazxkubdpn',
                'url': 'https://open.assembly.go.kr/portal/openapi/nzmimeepazxkubdpn',
                'filename': 'assembly_bills_age_{AGE}.json',
                'is_age_iteration': True
            }
        ]

    async def fetch_api_data(self, session: aiohttp.ClientSession, api: Dict[str, Any], 
                           extra_params: str = '') -> Dict[str, Any]:
        """Fetch data from a single API endpoint with pagination"""
        print(f"Starting data collection for {api['name']}...")
        
        all_data = []
        p_index = api.get('start_index', 1)
        has_more_data = True

        while has_more_data:
            try:
                url = f"{api['url']}?KEY={self.api_key}&Type=json&pIndex={p_index}&pSize={self.page_size}{extra_params}"
                print(f"Fetching {api['name']} - Page {p_index}...")
                
                async with session.get(url) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    # Extract the actual data array from the response
                    data_key = next(iter(data.keys()))
                    items = data.get(data_key, [None, {}])[1].get('row', []) if len(data.get(data_key, [])) > 1 else []
                    
                    if not items:
                        has_more_data = False
                        print(f"No more data for {api['name']} at page {p_index}")
                    else:
                        all_data.extend(items)
                        print(f"Collected {len(items)} items from {api['name']} page {p_index}")
                        p_index += 1
                    
                    # Add delay to avoid overwhelming the server
                    await asyncio.sleep(0.1)
                    
            except Exception as error:
                print(f"Error fetching {api['name']} page {p_index}: {error}")
                has_more_data = False

        return {
            'api': api['name'],
            'total_items': len(all_data),
            'data': all_data
        }

    async def fetch_daesu_data(self, session: aiohttp.ClientSession, api: Dict[str, Any], 
                             daesu: int) -> Dict[str, Any]:
        """Fetch data for a specific DAESU (assembly term)"""
        print(f"Starting data collection for {api['name']} DAESU {daesu}...")
        
        all_data = []
        p_index = 1
        has_more_data = True

        while has_more_data:
            try:
                url = f"{api['url']}?KEY={self.api_key}&Type=json&pIndex={p_index}&pSize={self.page_size}&DAESU={daesu}"
                print(f"Fetching {api['name']} DAESU {daesu} - Page {p_index}...")
                
                async with session.get(url) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    # Extract the actual data array from the response
                    data_key = next(iter(data.keys()))
                    items = data.get(data_key, [None, {}])[1].get('row', []) if len(data.get(data_key, [])) > 1 else []
                    
                    if not items:
                        has_more_data = False
                        print(f"No more data for {api['name']} DAESU {daesu} at page {p_index}")
                    else:
                        all_data.extend(items)
                        print(f"Collected {len(items)} items from {api['name']} DAESU {daesu} page {p_index}")
                        p_index += 1
                    
                    # Add delay to avoid overwhelming the server
                    await asyncio.sleep(0.1)
                    
            except Exception as error:
                print(f"Error fetching {api['name']} DAESU {daesu} page {p_index}: {error}")
                has_more_data = False

        return {
            'api': api['name'],
            'daesu': daesu,
            'total_items': len(all_data),
            'data': all_data
        }

    async def fetch_age_data(self, session: aiohttp.ClientSession, api: Dict[str, Any], 
                           age: int) -> Dict[str, Any]:
        """Fetch data for a specific AGE (assembly term)"""
        print(f"Starting data collection for {api['name']} AGE {age}...")
        
        all_data = []
        p_index = api.get('start_index', 1)
        has_more_data = True

        while has_more_data:
            try:
                url = f"{api['url']}?KEY={self.api_key}&Type=json&pIndex={p_index}&pSize={self.page_size}&AGE={age}"
                print(f"Fetching {api['name']} AGE {age} - Page {p_index}...")
                
                async with session.get(url) as response:
                    response.raise_for_status()
                    data = await response.json()
                    
                    # Extract the actual data array from the response
                    data_key = next(iter(data.keys()))
                    items = data.get(data_key, [None, {}])[1].get('row', []) if len(data.get(data_key, [])) > 1 else []
                    
                    if not items:
                        has_more_data = False
                        print(f"No more data for {api['name']} AGE {age} at page {p_index}")
                    else:
                        all_data.extend(items)
                        print(f"Collected {len(items)} items from {api['name']} AGE {age} page {p_index}")
                        p_index += 1
                    
                    # Add delay to avoid overwhelming the server
                    await asyncio.sleep(0.1)
                    
            except Exception as error:
                print(f"Error fetching {api['name']} AGE {age} page {p_index}: {error}")
                has_more_data = False

        return {
            'api': api['name'],
            'age': age,
            'total_items': len(all_data),
            'data': all_data
        }

    def save_to_file(self, result: Dict[str, Any], filename: str) -> None:
        """Save result data to JSON file"""
        try:
            file_path = self.base_dir / filename
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(result, f, ensure_ascii=False, indent=2)
            print(f"Saved {result['total_items']} items to {filename}")
        except Exception as error:
            print(f"Error saving to {filename}: {error}")

    async def run(self) -> None:
        """Main execution method"""
        print('Starting API data aggregation...')
        
        connector = aiohttp.TCPConnector(limit=10)
        timeout = aiohttp.ClientTimeout(total=30)
        
        async with aiohttp.ClientSession(connector=connector, timeout=timeout) as session:
            for api in self.apis:
                try:
                    if api.get('is_daesu_iteration'):
                        # Handle DAESU iteration for member history API
                        for daesu in range(10, 23):  # 10-22대
                            result = await self.fetch_daesu_data(session, api, daesu)
                            filename = api['filename'].replace('{DAESU}', str(daesu))
                            self.save_to_file(result, filename)
                            print(f"Completed {api['name']} DAESU {daesu}: {result['total_items']} total items\n")
                    
                    elif api.get('is_age_iteration'):
                        # Handle AGE iteration for bills API
                        for age in range(10, 23):  # 10-22대
                            result = await self.fetch_age_data(session, api, age)
                            filename = api['filename'].replace('{AGE}', str(age))
                            self.save_to_file(result, filename)
                            print(f"Completed {api['name']} AGE {age}: {result['total_items']} total items\n")
                    
                    else:
                        result = await self.fetch_api_data(session, api)
                        self.save_to_file(result, api['filename'])
                        print(f"Completed {api['name']}: {result['total_items']} total items\n")
                        
                except Exception as error:
                    print(f"Failed to process {api['name']}: {error}")
        
        print('All APIs processed successfully!')

def main():
    """Main function to run the assembly data fetcher"""
    fetcher = AssemblyDataFetcher()
    asyncio.run(fetcher.run())

if __name__ == "__main__":
    main()