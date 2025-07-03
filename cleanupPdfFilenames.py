import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Tuple
import argparse

class PdfFilenameCleanup:
    def __init__(self):
        self.base_dir = Path(__file__).parent
        self.downloads_dir = self.base_dir / 'pdf_downloads'

    def test_rename(self, original_filename: str) -> str:
        """Test function to rename a single PDF file"""
        print(f'Testing rename for: {original_filename}')
        
        # Find the 6th underscore
        underscore_count = 0
        cut_index = -1
        
        for i, char in enumerate(original_filename):
            if char == '_':
                underscore_count += 1
                if underscore_count == 6:
                    cut_index = i
                    break
        
        if cut_index == -1:
            print(f'No 6th underscore found in: {original_filename}')
            return original_filename  # Return original if less than 6 underscores
        
        # Extract everything before the 6th underscore and add .pdf extension
        base_name = original_filename[:cut_index]
        new_filename = base_name + '.pdf'
        
        print(f'Original: {original_filename}')
        print(f'New:      {new_filename}')
        print(f'Cut at position: {cut_index} (6th underscore)')
        
        return new_filename

    def generate_new_filename(self, filename: str) -> str:
        """Generate new filename by removing text after 6th underscore"""
        # Find the 6th underscore
        underscore_count = 0
        cut_index = -1
        
        for i, char in enumerate(filename):
            if char == '_':
                underscore_count += 1
                if underscore_count == 6:
                    cut_index = i
                    break
        
        if cut_index == -1:
            return filename  # Return original if less than 6 underscores
        
        # Extract everything before the 6th underscore and add .pdf extension
        base_name = filename[:cut_index]
        return base_name + '.pdf'

    def find_all_pdf_files(self, dir_path: Path) -> List[Dict[str, Any]]:
        """Recursively find all PDF files in a directory"""
        pdf_files = []
        
        def search_directory(current_path: Path, relative_path: str = ''):
            try:
                for item in current_path.iterdir():
                    if item.is_dir():
                        # Recursively search subdirectories
                        rel_path = str(Path(relative_path) / item.name) if relative_path else item.name
                        search_directory(item, rel_path)
                    elif item.is_file() and item.suffix.lower() == '.pdf':
                        rel_path = str(Path(relative_path) / item.name) if relative_path else item.name
                        pdf_files.append({
                            'full_path': item,
                            'relative_path': rel_path,
                            'filename': item.name,
                            'directory': item.parent
                        })
            except Exception as error:
                print(f'Error reading directory {current_path}: {error}')
        
        search_directory(dir_path)
        return pdf_files

    def rename_all_pdf_files(self, dry_run: bool = True) -> Dict[str, Any]:
        """Rename all PDF files in the pdf_downloads folder"""
        try:
            print('Starting PDF file renaming process...')
            print(f'Mode: {"DRY RUN (no actual renaming)" if dry_run else "ACTUAL RENAMING"}')
            
            # Check if pdf_downloads directory exists
            if not self.downloads_dir.exists():
                print('pdf_downloads directory not found!')
                return {'error': 'pdf_downloads directory not found'}
            
            print(f'Searching for PDF files in: {self.downloads_dir}')
            
            # Find all PDF files
            pdf_files = self.find_all_pdf_files(self.downloads_dir)
            print(f'Found {len(pdf_files)} PDF files to process')
            
            if not pdf_files:
                print('No PDF files found to rename')
                return {'message': 'No PDF files found'}
            
            renamed_count = 0
            skipped_count = 0
            error_count = 0
            results = []
            
            for file_info in pdf_files:
                try:
                    new_filename = self.generate_new_filename(file_info['filename'])
                    
                    if new_filename == file_info['filename']:
                        print(f'⏭️  Skipping (no change needed): {file_info["relative_path"]}')
                        skipped_count += 1
                        results.append({
                            'original': file_info['relative_path'],
                            'new': file_info['relative_path'],
                            'status': 'skipped',
                            'reason': 'No 6th underscore found or no change needed'
                        })
                        continue
                    
                    new_full_path = file_info['directory'] / new_filename
                    
                    # Check if target file already exists
                    if new_full_path.exists():
                        print(f'⚠️  Warning: Target file already exists, skipping: {new_filename}')
                        skipped_count += 1
                        new_relative = str(Path(file_info['relative_path']).parent / new_filename)
                        results.append({
                            'original': file_info['relative_path'],
                            'new': new_relative,
                            'status': 'skipped',
                            'reason': 'Target file already exists'
                        })
                        continue
                    
                    if not dry_run:
                        # Actually rename the file
                        file_info['full_path'].rename(new_full_path)
                        new_relative = str(Path(file_info['relative_path']).parent / new_filename)
                        print(f'✅ Renamed: {file_info["relative_path"]} → {new_relative}')
                    else:
                        new_relative = str(Path(file_info['relative_path']).parent / new_filename)
                        print(f'🔍 Would rename: {file_info["relative_path"]} → {new_relative}')
                    
                    renamed_count += 1
                    results.append({
                        'original': file_info['relative_path'],
                        'new': new_relative,
                        'status': 'would_rename' if dry_run else 'renamed',
                        'reason': 'Successfully processed'
                    })
                    
                except Exception as error:
                    print(f'❌ Error processing {file_info["relative_path"]}: {error}')
                    error_count += 1
                    results.append({
                        'original': file_info['relative_path'],
                        'new': None,
                        'status': 'error',
                        'reason': str(error)
                    })
            
            # Save results to file
            results_data = {
                'summary': {
                    'total_files': len(pdf_files),
                    'renamed_count': renamed_count,
                    'skipped_count': skipped_count,
                    'error_count': error_count,
                    'dry_run': dry_run,
                    'processed_date': datetime.now().isoformat()
                },
                'results': results
            }
            
            date_str = datetime.now().strftime('%Y-%m-%d')
            mode_str = 'dryrun' if dry_run else 'actual'
            results_path = self.base_dir / f'pdf_rename_results_{mode_str}_{date_str}.json'
            
            with open(results_path, 'w', encoding='utf-8') as f:
                json.dump(results_data, f, ensure_ascii=False, indent=2)
            
            print('\n=== Summary ===')
            print(f'Total files processed: {len(pdf_files)}')
            print(f'{"Would be renamed" if dry_run else "Renamed"}: {renamed_count}')
            print(f'Skipped: {skipped_count}')
            print(f'Errors: {error_count}')
            print(f'Results saved to: {results_path}')
            
            if dry_run:
                print('\n💡 This was a dry run. To actually rename files, use --actual flag')
            
            return results_data
            
        except Exception as error:
            print(f'Error in rename_all_pdf_files: {error}')
            raise error

def main():
    """Main function with command line argument parsing"""
    parser = argparse.ArgumentParser(description='Clean up PDF filenames by removing text after 6th underscore')
    parser.add_argument('--test', action='store_true', 
                       help='Test mode - test sample filename transformations')
    parser.add_argument('--actual', action='store_true', 
                       help='Perform actual renaming (default is dry run)')
    
    args = parser.parse_args()
    
    cleanup = PdfFilenameCleanup()
    
    if args.test:
        # Test mode - test a sample filename
        print('=== Testing filename transformation ===\n')
        
        test_filenames = [
            '국회본회의 회의록_047627_제20대_제354회_제13차_20171124_34.  에너지산업클러스터의 지정 및 육성에 관한 특별법안(장병완 의원 대표발의)(장병완ㆍ주승용ㆍ전혜숙ㆍ유동수ㆍ김경진ㆍ이동섭ㆍ박준영ㆍ정인화ㆍ박지원ㆍ김수민ㆍ김동철ㆍ박주선ㆍ송기석ㆍ이찬열ㆍ신용현ㆍ노웅래ㆍ이상돈ㆍ이태규ㆍ윤호중ㆍ천정배ㆍ윤영일ㆍ이개호ㆍ인재근ㆍ이채익ㆍ홍의락ㆍ최경환(국)ㆍ김민기ㆍ김중로ㆍ백재현ㆍ조배숙ㆍ권은희ㆍ손금주 의원 발의).pdf'
        ]
        
        for filename in test_filenames:
            cleanup.test_rename(filename)
            print('---\n')
    
    else:
        # Perform renaming (dry run by default, actual if --actual flag is used)
        dry_run = not args.actual
        results = cleanup.rename_all_pdf_files(dry_run)
        
        if not args.actual:
            print('\nDry run completed! Use --actual flag to perform actual renaming.')

if __name__ == "__main__":
    main()