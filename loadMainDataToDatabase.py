import os
import json
import pyodbc
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from dotenv import load_dotenv

class MainDataLoader:
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
        
        # Table schemas - 실제 JSON 데이터 구조에 맞게 수정
        self.schemas = {
            'assembly_bills': """
                CREATE TABLE assembly_bills (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    BILL_ID NVARCHAR(50),
                    BILL_NO NVARCHAR(50),
                    BILL_NAME NVARCHAR(500),
                    COMMITTEE NVARCHAR(200),
                    PROPOSE_DT DATE,
                    PROC_RESULT NVARCHAR(100),
                    AGE NVARCHAR(10),
                    DETAIL_LINK NVARCHAR(1000),
                    PROPOSER NVARCHAR(500),
                    MEMBER_LIST NVARCHAR(1000),
                    LAW_PROC_DT DATE,
                    LAW_PRESENT_DT DATE,
                    LAW_SUBMIT_DT DATE,
                    CMT_PROC_RESULT_CD NVARCHAR(100),
                    CMT_PROC_DT DATE,
                    CMT_PRESENT_DT DATE,
                    COMMITTEE_DT DATE,
                    PROC_DT DATE,
                    COMMITTEE_ID NVARCHAR(50),
                    PUBL_PROPOSER NVARCHAR(MAX),
                    LAW_PROC_RESULT_CD NVARCHAR(100),
                    RST_PROPOSER NVARCHAR(200),
                    age_number INT,
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """,
            'assembly_members_history': """
                CREATE TABLE assembly_members_history (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    MONA_CD NVARCHAR(50),
                    HG_NM NVARCHAR(100),
                    HJ_NM NVARCHAR(100),
                    ENG_NM NVARCHAR(200),
                    BTH_GBN_NM NVARCHAR(50),
                    BTH_DATE NVARCHAR(20),
                    AGED NVARCHAR(10),
                    JOB_RES_NM NVARCHAR(200),
                    POLY_NM NVARCHAR(100),
                    ORIG_NM NVARCHAR(100),
                    ELECT_GBN_NM NVARCHAR(100),
                    CMIT_NM NVARCHAR(200),
                    REELE_GBN_NM NVARCHAR(100),
                    UNITS NVARCHAR(100),
                    SEX_GBN_NM NVARCHAR(20),
                    TEL_NO NVARCHAR(50),
                    E_MAIL NVARCHAR(100),
                    HOMEPAGE NVARCHAR(200),
                    STAFF NVARCHAR(500),
                    SECRETARY NVARCHAR(200),
                    SECRETARY2 NVARCHAR(200),
                    ASSEM_ADDR NVARCHAR(300),
                    MEM_TITLE NTEXT,
                    DAESU INT,
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """,
            'assembly_members_history_daesu': """
                CREATE TABLE assembly_members_history_daesu (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    DAESU NVARCHAR(10),
                    DAE NTEXT,
                    DAE_NM NVARCHAR(100),
                    NAME NVARCHAR(100),
                    NAME_HAN NVARCHAR(100),
                    JA NVARCHAR(100),
                    HO NTEXT,
                    BIRTH NVARCHAR(50),
                    BON NVARCHAR(100),
                    POSI NVARCHAR(200),
                    HAK NTEXT,
                    HOBBY NVARCHAR(500),
                    BOOK NTEXT,
                    SANG NTEXT,
                    DEAD NVARCHAR(50),
                    URL NVARCHAR(500),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """,
            'assembly_members_integrated': """
                CREATE TABLE assembly_members_integrated (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    NAAS_CD NVARCHAR(50),
                    NAAS_NM NVARCHAR(100),
                    NAAS_CH_NM NVARCHAR(100),
                    NAAS_EN_NM NVARCHAR(200),
                    BIRDY_DIV_CD NVARCHAR(10),
                    BIRDY_DT NVARCHAR(20),
                    DTY_NM NVARCHAR(100),
                    PLPT_NM NVARCHAR(200),
                    ELECD_NM NVARCHAR(200),
                    ELECD_DIV_NM NVARCHAR(200),
                    CMIT_NM NVARCHAR(500),
                    BLNG_CMIT_NM NTEXT,
                    RLCT_DIV_NM NVARCHAR(100),
                    GTELT_ERACO NVARCHAR(100),
                    NTR_DIV NVARCHAR(10),
                    NAAS_TEL_NO NVARCHAR(50),
                    NAAS_EMAIL_ADDR NVARCHAR(100),
                    NAAS_HP_URL NVARCHAR(200),
                    AIDE_NM NVARCHAR(200),
                    CHF_SCRT_NM NVARCHAR(200),
                    SCRT_NM NVARCHAR(200),
                    BRF_HST NTEXT,
                    OFFM_RNUM_NO NVARCHAR(50),
                    NAAS_PIC NVARCHAR(500),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """,
            'assembly_members_profile': """
                CREATE TABLE assembly_members_profile (
                    id INT IDENTITY(1,1) PRIMARY KEY,
                    HG_NM NVARCHAR(100),
                    HJ_NM NVARCHAR(100),
                    ENG_NM NVARCHAR(200),
                    BTH_GBN_NM NVARCHAR(50),
                    BTH_DATE NVARCHAR(20),
                    JOB_RES_NM NVARCHAR(200),
                    POLY_NM NVARCHAR(100),
                    ORIG_NM NVARCHAR(100),
                    ELECT_GBN_NM NVARCHAR(100),
                    CMIT_NM NVARCHAR(200),
                    CMITS NVARCHAR(200),
                    REELE_GBN_NM NVARCHAR(100),
                    UNITS NVARCHAR(100),
                    SEX_GBN_NM NVARCHAR(20),
                    TEL_NO NVARCHAR(50),
                    E_MAIL NVARCHAR(100),
                    HOMEPAGE NVARCHAR(200),
                    STAFF NVARCHAR(500),
                    SECRETARY NVARCHAR(200),
                    SECRETARY2 NVARCHAR(500),
                    MONA_CD NVARCHAR(50),
                    MEM_TITLE NTEXT,
                    ASSEM_ADDR NVARCHAR(300),
                    created_at DATETIME2 DEFAULT GETDATE()
                )
            """
        }

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

    def create_tables(self, cursor) -> None:
        """Create database tables if they don't exist"""
        print('Creating tables...')
        
        for table_name, schema in self.schemas.items():
            try:
                # Check if table exists
                cursor.execute("""
                    SELECT COUNT(*) 
                    FROM INFORMATION_SCHEMA.TABLES 
                    WHERE TABLE_NAME = ?
                """, table_name)
                
                if cursor.fetchone()[0] == 0:
                    cursor.execute(schema)
                    print(f'Table {table_name} created.')
                else:
                    print(f'Table {table_name} already exists.')
            except Exception as error:
                print(f'Error creating table {table_name}: {error}')

    def parse_date(self, date_string: str) -> Optional[str]:
        """Parse date string to SQL Server compatible format"""
        if not date_string or date_string == 'null':
            return None
        try:
            # Try to parse the date and return in ISO format
            dt = datetime.fromisoformat(date_string.replace('Z', '+00:00'))
            return dt.strftime('%Y-%m-%d')
        except:
            return None

    def insert_bills_data(self, cursor, data: List[Dict[str, Any]], metadata: Dict[str, Any]) -> None:
        """Insert bills data into database"""
        print(f'Inserting {len(data)} bills records...')
        
        for item in data:
            try:
                cursor.execute("""
                    INSERT INTO assembly_bills (
                        BILL_ID, BILL_NO, BILL_NAME, COMMITTEE, PROPOSE_DT, PROC_RESULT, AGE,
                        DETAIL_LINK, PROPOSER, MEMBER_LIST, LAW_PROC_DT, LAW_PRESENT_DT, LAW_SUBMIT_DT,
                        CMT_PROC_RESULT_CD, CMT_PROC_DT, CMT_PRESENT_DT, COMMITTEE_DT, PROC_DT,
                        COMMITTEE_ID, PUBL_PROPOSER, LAW_PROC_RESULT_CD, RST_PROPOSER, age_number
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    item.get('BILL_ID'),
                    item.get('BILL_NO'),
                    item.get('BILL_NAME'),
                    item.get('COMMITTEE'),
                    self.parse_date(item.get('PROPOSE_DT')),
                    item.get('PROC_RESULT'),
                    item.get('AGE'),
                    item.get('DETAIL_LINK'),
                    item.get('PROPOSER'),
                    item.get('MEMBER_LIST'),
                    self.parse_date(item.get('LAW_PROC_DT')),
                    self.parse_date(item.get('LAW_PRESENT_DT')),
                    self.parse_date(item.get('LAW_SUBMIT_DT')),
                    item.get('CMT_PROC_RESULT_CD'),
                    self.parse_date(item.get('CMT_PROC_DT')),
                    self.parse_date(item.get('CMT_PRESENT_DT')),
                    self.parse_date(item.get('COMMITTEE_DT')),
                    self.parse_date(item.get('PROC_DT')),
                    item.get('COMMITTEE_ID'),
                    item.get('PUBL_PROPOSER'),
                    item.get('LAW_PROC_RESULT_CD'),
                    item.get('RST_PROPOSER'),
                    metadata.get('age')
                ))
            except Exception as error:
                print(f'Error inserting bill record: {error}')

    def insert_members_history_data(self, cursor, data: List[Dict[str, Any]], metadata: Dict[str, Any]) -> None:
        """Insert members history data into database"""
        print(f'Inserting {len(data)} members history records...')
        
        for item in data:
            try:
                cursor.execute("""
                    INSERT INTO assembly_members_history (
                        MONA_CD, HG_NM, HJ_NM, ENG_NM, BTH_GBN_NM, BTH_DATE, AGED, JOB_RES_NM, POLY_NM, ORIG_NM,
                        ELECT_GBN_NM, CMIT_NM, REELE_GBN_NM, UNITS, SEX_GBN_NM, TEL_NO,
                        E_MAIL, HOMEPAGE, STAFF, SECRETARY, SECRETARY2, ASSEM_ADDR, MEM_TITLE, DAESU
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    item.get('MONA_CD'),
                    item.get('HG_NM'),
                    item.get('HJ_NM'),
                    item.get('ENG_NM'),
                    item.get('BTH_GBN_NM'),
                    item.get('BTH_DATE'),
                    item.get('AGED'),
                    item.get('JOB_RES_NM'),
                    item.get('POLY_NM'),
                    item.get('ORIG_NM'),
                    item.get('ELECT_GBN_NM'),
                    item.get('CMIT_NM'),
                    item.get('REELE_GBN_NM'),
                    item.get('UNITS'),
                    item.get('SEX_GBN_NM'),
                    item.get('TEL_NO'),
                    item.get('E_MAIL'),
                    item.get('HOMEPAGE'),
                    item.get('STAFF'),
                    item.get('SECRETARY'),
                    item.get('SECRETARY2'),
                    item.get('ASSEM_ADDR'),
                    item.get('MEM_TITLE'),
                    metadata.get('daesu')
                ))
            except Exception as error:
                print(f'Error inserting member history record: {error}')

    def insert_members_history_daesu_data(self, cursor, data: List[Dict[str, Any]]) -> None:
        """Insert members history daesu data into database"""
        print(f'Inserting {len(data)} members history daesu records...')
        
        for item in data:
            try:
                cursor.execute("""
                    INSERT INTO assembly_members_history_daesu (
                        DAESU, DAE, DAE_NM, NAME, NAME_HAN, JA, HO, BIRTH, BON, POSI,
                        HAK, HOBBY, BOOK, SANG, DEAD, URL
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    item.get('DAESU'),
                    item.get('DAE'),
                    item.get('DAE_NM'),
                    item.get('NAME'),
                    item.get('NAME_HAN'),
                    item.get('JA'),
                    item.get('HO'),
                    item.get('BIRTH'),
                    item.get('BON'),
                    item.get('POSI'),
                    item.get('HAK'),
                    item.get('HOBBY'),
                    item.get('BOOK'),
                    item.get('SANG'),
                    item.get('DEAD'),
                    item.get('URL')
                ))
            except Exception as error:
                print(f'Error inserting member history daesu record: {error}')

    def insert_members_integrated_data(self, cursor, data: List[Dict[str, Any]]) -> None:
        """Insert integrated members data into database"""
        print(f'Inserting {len(data)} integrated members records...')
        
        for item in data:
            try:
                cursor.execute("""
                    INSERT INTO assembly_members_integrated (
                        NAAS_CD, NAAS_NM, NAAS_CH_NM, NAAS_EN_NM, BIRDY_DIV_CD, BIRDY_DT, DTY_NM,
                        PLPT_NM, ELECD_NM, ELECD_DIV_NM, CMIT_NM, BLNG_CMIT_NM, RLCT_DIV_NM,
                        GTELT_ERACO, NTR_DIV, NAAS_TEL_NO, NAAS_EMAIL_ADDR, NAAS_HP_URL,
                        AIDE_NM, CHF_SCRT_NM, SCRT_NM, BRF_HST, OFFM_RNUM_NO, NAAS_PIC
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    item.get('NAAS_CD'),
                    item.get('NAAS_NM'),
                    item.get('NAAS_CH_NM'),
                    item.get('NAAS_EN_NM'),
                    item.get('BIRDY_DIV_CD'),
                    item.get('BIRDY_DT'),
                    item.get('DTY_NM'),
                    item.get('PLPT_NM'),
                    item.get('ELECD_NM'),
                    item.get('ELECD_DIV_NM'),
                    item.get('CMIT_NM'),
                    item.get('BLNG_CMIT_NM'),
                    item.get('RLCT_DIV_NM'),
                    item.get('GTELT_ERACO'),
                    item.get('NTR_DIV'),
                    item.get('NAAS_TEL_NO'),
                    item.get('NAAS_EMAIL_ADDR'),
                    item.get('NAAS_HP_URL'),
                    item.get('AIDE_NM'),
                    item.get('CHF_SCRT_NM'),
                    item.get('SCRT_NM'),
                    item.get('BRF_HST'),
                    item.get('OFFM_RNUM_NO'),
                    item.get('NAAS_PIC')
                ))
            except Exception as error:
                print(f'Error inserting integrated member record: {error}')

    def insert_members_profile_data(self, cursor, data: List[Dict[str, Any]]) -> None:
        """Insert profile data into database"""
        print(f'Inserting {len(data)} profile records...')
        
        for item in data:
            try:
                cursor.execute("""
                    INSERT INTO assembly_members_profile (
                        HG_NM, HJ_NM, ENG_NM, BTH_GBN_NM, BTH_DATE, JOB_RES_NM, POLY_NM, ORIG_NM,
                        ELECT_GBN_NM, CMIT_NM, CMITS, REELE_GBN_NM, UNITS, SEX_GBN_NM, TEL_NO,
                        E_MAIL, HOMEPAGE, STAFF, SECRETARY, SECRETARY2, MONA_CD, MEM_TITLE, ASSEM_ADDR
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    item.get('HG_NM'),
                    item.get('HJ_NM'),
                    item.get('ENG_NM'),
                    item.get('BTH_GBN_NM'),
                    item.get('BTH_DATE'),
                    item.get('JOB_RES_NM'),
                    item.get('POLY_NM'),
                    item.get('ORIG_NM'),
                    item.get('ELECT_GBN_NM'),
                    item.get('CMIT_NM'),
                    item.get('CMITS'),
                    item.get('REELE_GBN_NM'),
                    item.get('UNITS'),
                    item.get('SEX_GBN_NM'),
                    item.get('TEL_NO'),
                    item.get('E_MAIL'),
                    item.get('HOMEPAGE'),
                    item.get('STAFF'),
                    item.get('SECRETARY'),
                    item.get('SECRETARY2'),
                    item.get('MONA_CD'),
                    item.get('MEM_TITLE'),
                    item.get('ASSEM_ADDR')
                ))
            except Exception as error:
                print(f'Error inserting profile record: {error}')

    def load_json_file(self, file_path: Path) -> Optional[Dict[str, Any]]:
        """Load JSON file"""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except Exception as error:
            print(f'Error reading file {file_path}: {error}')
            return None

    def run(self) -> None:
        """Main execution method"""
        try:
            print('Connecting to SQL Server Database...')
            connection = self.get_connection()
            cursor = connection.cursor()
            print('Connected successfully!')
            
            self.create_tables(cursor)
            
            # Get all assembly JSON files
            files = [f for f in self.base_dir.iterdir() if f.is_file()]
            assembly_files = [f for f in files if f.name.startswith('assembly_') and f.name.endswith('.json')]
            
            print(f'Found {len(assembly_files)} assembly JSON files to process.')
            
            for file_path in assembly_files:
                print(f'\nProcessing file: {file_path.name}')
                json_data = self.load_json_file(file_path)
                
                if not json_data or 'data' not in json_data or not json_data['data']:
                    print(f'Skipping {file_path.name} - no data found or empty data array.')
                    continue
                
                metadata = {
                    'daesu': json_data.get('daesu'),
                    'age': json_data.get('age')
                }
                
                # Determine table based on filename
                if 'bills' in file_path.name:
                    # Skip bills data loading (commented out in original JS)
                    pass
                elif 'members_history_daesu' in file_path.name:
                    self.insert_members_history_daesu_data(cursor, json_data['data'])
                elif 'history' in file_path.name:
                    self.insert_members_history_data(cursor, json_data['data'], metadata)
                elif 'integrated' in file_path.name:
                    self.insert_members_integrated_data(cursor, json_data['data'])
                elif 'profile' in file_path.name:
                    self.insert_members_profile_data(cursor, json_data['data'])
                
                print(f'Completed processing {file_path.name}')
                connection.commit()  # Commit after each file
            
            print('\nAll data loaded successfully!')
            
        except Exception as error:
            print(f'Error: {error}')
            if 'connection' in locals():
                connection.rollback()
        finally:
            if 'connection' in locals():
                connection.close()
                print('Database connection closed.')

def main():
    """Main function to run the data loader"""
    loader = MainDataLoader()
    loader.run()

if __name__ == "__main__":
    main()