import os
# import magic  # Optional dependency, will use file extension fallback
import zipfile
import tempfile
from pathlib import Path

class FileUtils:
    """Utility functions for file operations"""
    
    def __init__(self):
        self.supported_extensions = [
            '.zip', '.txt', '.docx', '.doc', '.pdf', 
            '.xlsx', '.xls', '.csv', '.json', '.xml'
        ]
        
        # MIME types for supported files
        self.supported_mime_types = [
            'application/zip',
            'text/plain',
            'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'application/msword',
            'application/pdf',
            'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            'application/vnd.ms-excel',
            'text/csv',
            'application/json',
            'application/xml',
            'text/xml'
        ]
        
        # Known data sources/sinks from the user's environment
        self.known_data_sources = {
            # Oracle sources
            'agilist_datalab_test': {'type': 'Oracle', 'category': 'source'},
            'AGILIST': {'type': 'Oracle', 'category': 'source'},
            'AGILIST_DATALAB': {'type': 'Oracle', 'category': 'source'},
            'AGILIST_DELV': {'type': 'Oracle', 'category': 'source'},
            'AGLPRD': {'type': 'Oracle', 'category': 'source'},
            'AGLPRD_IM_ATM': {'type': 'Oracle', 'category': 'source'},
            'AGLPRDDLV': {'type': 'Oracle', 'category': 'source'},
            'AGLPRDIMSTAGE': {'type': 'Oracle', 'category': 'source'},
            'BOXI': {'type': 'Oracle', 'category': 'source'},
            'prdpi': {'type': 'Oracle', 'category': 'source'},
            # Azure Data Lake Storage
            'AzureDataLakeStorage1': {'type': 'Azure Data Lake Storage Gen2', 'category': 'sink'},
            # SQL Server sources
            'Dataprd': {'type': 'SQL Server', 'category': 'source'},
            'ResusApp': {'type': 'SQL Server', 'category': 'source'},
            'SPHDSQLPRD11': {'type': 'SQL Server', 'category': 'source'}
        }
    
    def is_supported_file(self, file_path):
        """Check if file is supported based on extension and MIME type"""
        try:
            # Check extension
            file_extension = Path(file_path).suffix.lower()
            if file_extension not in self.supported_extensions:
                return False
            
            # Additional MIME type check if magic is available
            try:
                # Try to import magic if available
                import magic
                mime_type = magic.from_file(file_path, mime=True)
                return mime_type in self.supported_mime_types
            except:
                # If magic fails, rely on extension only
                return True
                
        except Exception:
            return False
    
    def get_file_info(self, file_path):
        """Get detailed file information"""
        try:
            file_stats = os.stat(file_path)
            file_info = {
                'name': os.path.basename(file_path),
                'path': file_path,
                'size': file_stats.st_size,
                'extension': Path(file_path).suffix.lower(),
                'is_supported': self.is_supported_file(file_path)
            }
            
            # Try to get MIME type
            try:
                import magic
                file_info['mime_type'] = magic.from_file(file_path, mime=True)
            except:
                file_info['mime_type'] = 'unknown'
            
            return file_info
            
        except Exception as e:
            return {
                'name': os.path.basename(file_path),
                'path': file_path,
                'error': str(e)
            }
    
    def extract_zip_safely(self, zip_path, extract_to=None):
        """Safely extract zip file with size and path validation"""
        if extract_to is None:
            extract_to = tempfile.mkdtemp()
        
        extracted_files = []
        
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                # Validate zip file
                zip_info_list = zip_ref.infolist()
                
                for zip_info in zip_info_list:
                    # Skip directories
                    if zip_info.is_dir():
                        continue
                    
                    # Security check: prevent directory traversal
                    if '..' in zip_info.filename or zip_info.filename.startswith('/'):
                        print(f"Skipping suspicious file: {zip_info.filename}")
                        continue
                    
                    # Size check: skip files larger than 100MB
                    if zip_info.file_size > 100 * 1024 * 1024:
                        print(f"Skipping large file: {zip_info.filename} ({zip_info.file_size} bytes)")
                        continue
                    
                    # Extract file
                    try:
                        zip_ref.extract(zip_info, extract_to)
                        extracted_path = os.path.join(extract_to, zip_info.filename)
                        
                        if self.is_supported_file(extracted_path):
                            extracted_files.append({
                                'original_name': zip_info.filename,
                                'extracted_path': extracted_path,
                                'size': zip_info.file_size,
                                'info': self.get_file_info(extracted_path)
                            })
                    except Exception as e:
                        print(f"Error extracting {zip_info.filename}: {str(e)}")
                        continue
            
            return extracted_files
            
        except Exception as e:
            print(f"Error extracting zip file: {str(e)}")
            return []
    
    def clean_temp_files(self, file_paths):
        """Clean up temporary files"""
        for file_path in file_paths:
            try:
                if os.path.exists(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(f"Error cleaning up {file_path}: {str(e)}")
    
    def validate_file_size(self, file_path, max_size_mb=50):
        """Validate file size"""
        try:
            file_size = os.path.getsize(file_path)
            max_size_bytes = max_size_mb * 1024 * 1024
            return file_size <= max_size_bytes, file_size
        except Exception:
            return False, 0
    
    def get_text_preview(self, file_path, max_chars=500):
        """Get a preview of text content from file"""
        try:
            if not self.is_supported_file(file_path):
                return "Unsupported file type"
            
            extension = Path(file_path).suffix.lower()
            
            if extension == '.txt':
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read(max_chars)
                    return content + "..." if len(content) == max_chars else content
            
            # For other file types, return basic info
            file_info = self.get_file_info(file_path)
            return f"File: {file_info['name']}\nSize: {file_info['size']} bytes\nType: {file_info.get('mime_type', 'unknown')}"
            
        except Exception as e:
            return f"Error reading file: {str(e)}"
    
    def create_temp_copy(self, source_path):
        """Create a temporary copy of a file"""
        try:
            temp_dir = tempfile.gettempdir()
            temp_name = f"temp_{os.path.basename(source_path)}"
            temp_path = os.path.join(temp_dir, temp_name)
            
            # Copy file
            with open(source_path, 'rb') as src, open(temp_path, 'wb') as dst:
                dst.write(src.read())
            
            return temp_path
            
        except Exception as e:
            print(f"Error creating temp copy: {str(e)}")
            return None
