import os
import json
import pyodbc
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

class VoteDataLoader:
    def __init__(self):
        # Load environment variables
        load_dotenv()
        
        self.base_dir = Path(__file__).parent
        
        # Database configuration
        self.db_config = {
            'server': os.getenv('DB_SERVER'),
            'database': os.getenv('DB_DATABASE'),
            'username': os.getenv('DB_USERNAME'),
            'password': os.getenv('DB_PASSWORD'),
            'driver': '{ODBC Driver 17 for SQL Server}'  # or '{SQL Server}'
        }
        
        # Table schema for plenary session vote data
        self.vote_table_schema = """
            CREATE TABLE assembly_plenary_session_vote (
                id INT IDENTITY(1,1) PRIMARY KEY,
                HG_NM NVARCHAR(100),
                HJ_NM NVARCHAR(100),
                POLY_NM NVARCHAR(100),
                ORIG_NM NVARCHAR(100),
                MEMBER_NO NVARCHAR(50),
                POLY_CD NVARCHAR(50),
                ORIG_CD NVARCHAR(50),
                VOTE_DATE NVARCHAR(50),
                BILL_NO NVARCHAR(50),
                BILL_NAME NVARCHAR(500),
                BILL_ID NVARCHAR(100),
                LAW_TITLE NVARCHAR(500),
                CURR_COMMITTEE NVARCHAR(200),
                RESULT_VOTE_MOD NVARCHAR(50),
                DEPT_CD NVARCHAR(50),
                CURR_COMMITTEE_ID NVARCHAR(50),
                DISP_ORDER INT,
                BILL_URL NVARCHAR(1000),
                BILL_NAME_URL NVARCHAR(1000),
                SESSION_CD INT,
                CURRENTS_CD INT,
                AGE INT,
                MONA_CD NVARCHAR(50),
                created_at DATETIME2 DEFAULT GETDATE()
            )
        """

    def get_connection(self):
        """Create database connection"""
        connection_string = (
            f"DRIVER={self.db_config['driver']};"
            f"SERVER={self.db_config['server']};"
            f"DATABASE={self.db_config['database']};"
            f"UID={self.db_config['username']};"
            f"PWD={self.db_config['password']};"
            "Encrypt=yes;TrustServerCertificate=no;"
        )
        return pyodbc.connect(connection_string)

    def create_vote_table(self, cursor) -> None:
        """Create assembly_plenary_session_vote table if it doesn't exist"""
        print('Creating assembly_plenary_session_vote table...')
        
        try:
            # Check if table exists
            cursor.execute("""
                SELECT COUNT(*) 
                FROM INFORMATION_SCHEMA.TABLES 
                WHERE TABLE_NAME = 'assembly_plenary_session_vote'
            """)
            
            if cursor.fetchone()[0] == 0:
                cursor.execute(self.vote_table_schema)
                print('Table assembly_plenary_session_vote created.')
            else:
                print('Table assembly_plenary_session_vote already exists.')
        except Exception as error:
            print(f'Error creating vote table: {error}')
            raise error

    def insert_vote_data(self, cursor, vote_item: Dict[str, Any], original_bill_id: str, 
                        original_age: str, api_status: str) -> None:
        """Insert a single vote record into the database"""
        try:
            cursor.execute("""
                INSERT INTO assembly_plenary_session_vote (
                    HG_NM, HJ_NM, POLY_NM, ORIG_NM, MEMBER_NO, POLY_CD, ORIG_CD, VOTE_DATE,
                    BILL_NO, BILL_NAME, BILL_ID, LAW_TITLE, CURR_COMMITTEE, RESULT_VOTE_MOD,
                    DEPT_CD, CURR_COMMITTEE_ID, DISP_ORDER, BILL_URL, BILL_NAME_URL,
                    SESSION_CD, CURRENTS_CD, AGE, MONA_CD
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                vote_item.get('HG_NM'),
                vote_item.get('HJ_NM'),
                vote_item.get('POLY_NM'),
                vote_item.get('ORIG_NM'),
                vote_item.get('MEMBER_NO'),
                vote_item.get('POLY_CD'),
                vote_item.get('ORIG_CD'),
                vote_item.get('VOTE_DATE'),
                vote_item.get('BILL_NO'),
                vote_item.get('BILL_NAME'),
                vote_item.get('BILL_ID'),
                vote_item.get('LAW_TITLE'),
                vote_item.get('CURR_COMMITTEE'),
                vote_item.get('RESULT_VOTE_MOD'),
                vote_item.get('DEPT_CD'),
                vote_item.get('CURR_COMMITTEE_ID'),
                vote_item.get('DISP_ORDER'),
                vote_item.get('BILL_URL'),
                vote_item.get('BILL_NAME_URL'),
                vote_item.get('SESSION_CD'),
                vote_item.get('CURRENTS_CD'),
                vote_item.get('AGE'),
                vote_item.get('MONA_CD')
            ))
        except Exception as error:
            print(f'Error inserting vote record: {error}')
            raise error

    def load_api_results_data(self) -> Dict[str, Any]:
        """Load API results data from JSON file"""
        try:
            print('Loading API results data...')
            
            # Try to load main results file first
            main_path = self.base_dir / 'assembly_bills_api_results.json'
            if main_path.exists():
                with open(main_path, 'r', encoding='utf-8') as f:
                    api_data = json.load(f)
                data_source = 'assembly_bills_api_results.json'
                print('Loaded data from main results file')
            else:
                # Fallback to temp file
                temp_path = self.base_dir / 'assembly_bills_api_results_temp.json'
                if temp_path.exists():
                    with open(temp_path, 'r', encoding='utf-8') as f:
                        api_data = json.load(f)
                    data_source = 'assembly_bills_api_results_temp.json'
                    print('Loaded data from temp results file')
                else:
                    raise Exception('Neither main nor temp API results file found')
            
            if 'results' not in api_data or not isinstance(api_data['results'], list):
                raise Exception('No results array found in API data')
            
            print(f'Data source: {data_source}')
            print(f'Found {len(api_data["results"])} API result records')
            
            return api_data
            
        except Exception as error:
            print(f'Error loading API results data: {error}')
            raise error

    def process_api_results(self, cursor, api_data: Dict[str, Any]) -> Dict[str, int]:
        """Process API results for vote data"""
        print('Processing API results for vote data...')
        
        processed_count = 0
        successful_inserts = 0
        vote_records_found = 0
        
        for result in api_data['results']:
            processed_count += 1
            
            if processed_count % 100 == 0:
                print(f'Processed {processed_count}/{len(api_data["results"])} records...')
            
            # Only process successful API responses
            if result.get('status') != 'success' or not result.get('api_response'):
                continue
            
            try:
                # Check if api_response has the expected structure
                if not isinstance(result['api_response'], list) or len(result['api_response']) < 2:
                    continue
                
                # Get the second element (index 1) from api_response array
                vote_data = result['api_response'][1]
                
                if not vote_data or not isinstance(vote_data.get('row'), list):
                    continue
                
                # Process each vote record in the row array
                for vote_item in vote_data['row']:
                    vote_records_found += 1
                    
                    try:
                        self.insert_vote_data(
                            cursor,
                            vote_item,
                            result['BILL_ID'],
                            result['AGE'],
                            result['status']
                        )
                        successful_inserts += 1
                    except Exception as insert_error:
                        print(f'Error inserting vote record for BILL_ID {result["BILL_ID"]}: {insert_error}')
                
            except Exception as error:
                print(f'Error processing result for BILL_ID {result["BILL_ID"]}: {error}')
        
        print(f'\nProcessing completed:')
        print(f'- Total API results processed: {processed_count}')
        print(f'- Vote records found: {vote_records_found}')
        print(f'- Successful database insertions: {successful_inserts}')
        
        return {
            'processed_count': processed_count,
            'vote_records_found': vote_records_found,
            'successful_inserts': successful_inserts
        }

    def run(self) -> None:
        """Main execution method"""
        try:
            print('Connecting to SQL Server Database...')
            connection = self.get_connection()
            cursor = connection.cursor()
            print('Connected successfully!')
            
            # Create vote table
            self.create_vote_table(cursor)
            
            # Load API results data
            api_data = self.load_api_results_data()
            
            # Process and insert vote data
            results = self.process_api_results(cursor, api_data)
            
            # Commit all changes
            connection.commit()
            
            print('\nData loading completed successfully!')
            print(f'Summary: {results["successful_inserts"]} vote records inserted from {results["vote_records_found"]} found records')
            
        except Exception as error:
            print(f'Error: {error}')
            if 'connection' in locals():
                connection.rollback()
            raise
        finally:
            if 'connection' in locals():
                connection.close()
                print('Database connection closed.')

def main():
    """Main function to run the vote data loader"""
    loader = VoteDataLoader()
    loader.run()

if __name__ == "__main__":
    main()