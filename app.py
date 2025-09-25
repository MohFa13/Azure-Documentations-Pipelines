from document_processor import DocumentProcessor

import streamlit as st
import os
import tempfile
import zipfile
from io import BytesIO
from pathlib import Path
import pandas as pd
from llm_handler_simple import SimpleLLMHandler as QwenLLMHandler
from template_generator import AzureTemplateGenerator
from file_utils import FileUtils
import traceback


import difflib
from docx import Document

def fix_reference_names(pipeline_json, available_names):
    """Fixes input/output reference names based on closest available source/sink names."""
    def closest(name):
        matches = difflib.get_close_matches(name, available_names, n=1, cutoff=0.4)
        return matches[0] if matches else name

    for activity in pipeline_json.get("properties", {}).get("activities", []):
        for inp in activity.get("inputs", []):
            if "referenceName" in inp:
                inp["referenceName"] = closest(inp["referenceName"])
        for out in activity.get("outputs", []):
            if "referenceName" in out:
                out["referenceName"] = closest(out["referenceName"])
    return pipeline_json

def extract_sources_from_docx(docx_path):
    """Extract source/sink names from Section 2 table in the documentation."""
    doc = Document(docx_path)
    names = []
    for table in doc.tables:
        for row in table.rows:
            cells = [c.text.strip() for c in row.cells]
            if cells and cells[0] and cells[0] not in ("Source/Sink Name", "Database", "Data Warehouse"):
                names.append(cells[0])
    return list(set(names))


# Set page configuration
st.set_page_config(
    page_title="DocuFillGenie - Azure Synapse Pipeline Documentation",
    page_icon="📄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'processing_complete' not in st.session_state:
    st.session_state.processing_complete = False
if 'generated_doc' not in st.session_state:
    st.session_state.generated_doc = None
if 'doc_path' not in st.session_state:
    st.session_state.doc_path = None

def initialize_components():
    """Initialize all required components"""
    try:
        if 'llm_handler' not in st.session_state:
            with st.spinner("Initializing Qwen model (this may take a few minutes on first run)..."):
                st.session_state.llm_handler = QwenLLMHandler()
        
        if 'doc_processor' not in st.session_state:
            st.session_state.doc_processor = DocumentProcessor()
        
        if 'template_generator' not in st.session_state:
            st.session_state.template_generator = AzureTemplateGenerator()
        
        if 'file_utils' not in st.session_state:
            st.session_state.file_utils = FileUtils()
            
        return True
    except Exception as e:
        st.error(f"Failed to initialize components: {str(e)}")
        return False

def process_uploaded_files(uploaded_files):
    """Process uploaded files and extract information"""
    try:
        all_extracted_data = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        for i, uploaded_file in enumerate(uploaded_files):
            status_text.text(f"Processing {uploaded_file.name}...")
            
            # Save uploaded file temporarily
            with tempfile.NamedTemporaryFile(delete=False, suffix=Path(uploaded_file.name).suffix) as tmp_file:
                tmp_file.write(uploaded_file.getvalue())
                tmp_file_path = tmp_file.name
            
            try:
                if uploaded_file.name.endswith('.zip'):
                    # Extract and process zip file
                    extract_dir = tempfile.mkdtemp()
                    with zipfile.ZipFile(tmp_file_path, 'r') as zip_ref:
                        zip_ref.extractall(extract_dir)
                    
                    # Process all files in the extracted directory
                    for root, dirs, files in os.walk(extract_dir):
                        for file in files:
                            file_path = os.path.join(root, file)
                            if st.session_state.file_utils.is_supported_file(file_path):
                                content = st.session_state.doc_processor.extract_content(file_path)
                                if content:
                                    all_extracted_data.append({
                                        'filename': file,
                                        'content': content,
                                        'source': uploaded_file.name
                                    })
                else:
                    # Process individual file
                    content = st.session_state.doc_processor.extract_content(tmp_file_path)
                    if content:
                        all_extracted_data.append({
                            'filename': uploaded_file.name,
                            'content': content,
                            'source': uploaded_file.name
                        })
                
                # Update progress
                progress_bar.progress((i + 1) / len(uploaded_files))
                
            finally:
                # Clean up temporary file
                os.unlink(tmp_file_path)
        
        status_text.text("Processing complete!")
        return all_extracted_data
        
    except Exception as e:
        st.error(f"Error processing files: {str(e)}")
        return []

def analyze_with_llm(extracted_data):
    """Analyze extracted data using Qwen LLM"""
    try:
        st.subheader("🤖 AI Analysis")
        progress_bar = st.progress(0)
        status_text = st.empty()
        
        analysis_results = []
        
        for i, data in enumerate(extracted_data):
            status_text.text(f"Analyzing {data['filename']} with Qwen model...")
            
            # Analyze content with LLM
            analysis = st.session_state.llm_handler.analyze_document(
                data['content'], 
                data['filename']
            )
            
            analysis_results.append({
                'filename': data['filename'],
                'content': data['content'],
                'analysis': analysis,
                'source': data['source']
            })
            
            progress_bar.progress((i + 1) / len(extracted_data))
        
        status_text.text("AI analysis complete!")
        return analysis_results
        
    except Exception as e:
        st.error(f"Error during AI analysis: {str(e)}")
        return []

def generate_documentation(analysis_results, uploaded_screenshots=None):
    """Generate Azure Synapse Pipeline documentation"""
    try:
        st.subheader("📝 Generating Documentation")
        
        with st.spinner("Creating Azure Synapse Pipeline documentation..."):
            # Combine all analysis results
            combined_analysis = st.session_state.llm_handler.synthesize_pipeline_info(analysis_results)
            
            # Generate Word document with optional screenshots
            doc_path = st.session_state.template_generator.generate_document(combined_analysis, uploaded_screenshots)
            
            st.session_state.doc_path = doc_path
            st.session_state.processing_complete = True
            
            st.success("Documentation generated successfully!")
            return doc_path
            
    except Exception as e:
        st.error(f"Error generating documentation: {str(e)}")
        return None

def main():
    st.title("🔧 Azure Data Factory Pipeline Analyzer")
    st.markdown("Upload a ZIP file containing Azure Data Factory pipeline JSON files, or individual files")
    
    # Initialize the DocumentProcessor
    try:
        processor = DocumentProcessor()
        st.success("✅ DocumentProcessor initialized successfully!")
    except Exception as e:
        st.error(f"❌ Failed to initialize DocumentProcessor: {e}")
        return
    
    # File uploader
    uploaded_file = st.file_uploader(
        "Choose a file", 
        type=['zip', 'json', 'pdf', 'docx', 'txt'],
        help="Upload a ZIP file containing pipeline folders with JSON files, a single JSON file, or other document types"
    )
    
    if uploaded_file is not None:
        try:
            # Check file type and process accordingly
            if uploaded_file.name.endswith('.zip'):
                st.write("### 📦 Processing ZIP file...")
                
                # Show zip contents for debugging
                with st.expander("📁 ZIP file contents (click to expand)"):
                    zip_bytes = BytesIO(uploaded_file.getvalue())
                    with zipfile.ZipFile(zip_bytes, 'r') as zip_ref:
                        all_files = zip_ref.namelist()
                        pipeline_files = [f for f in all_files if 'pipeline' in f.lower()]
                        json_files = [f for f in all_files if f.endswith('.json')]
                        
                        st.write("**All files in ZIP:**")
                        for file in all_files[:15]:  # Show first 15 files
                            st.write(f"- {file}")
                        if len(all_files) > 15:
                            st.write(f"... and {len(all_files) - 15} more files")
                        
                        st.write("**Pipeline-related files:**")
                        for file in pipeline_files:
                            st.write(f"- {file}")
                        
                        st.write("**JSON files:**")
                        for file in json_files[:10]:
                            st.write(f"- {file}")
                        if len(json_files) > 10:
                            st.write(f"... and {len(json_files) - 10} more JSON files")
                
                pipeline_data = processor.process_zip_file(uploaded_file)
                
                if pipeline_data:
                    st.write(f"### 📊 Extracted {len(pipeline_data)} activities from ZIP file")
                    
                    # Summary statistics
                    successful_activities = [a for a in pipeline_data if 'error' not in a]
                    error_activities = [a for a in pipeline_data if 'error' in a]
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("✅ Successful", len(successful_activities))
                    with col2:
                        st.metric("❌ Errors", len(error_activities))
                    with col3:
                        st.metric("📁 Total", len(pipeline_data))
                    
                    # Group by folder for better organization
                    folders = {}
                    for activity in pipeline_data:
                        folder = activity.get('source_folder', 'root')
                        if folder not in folders:
                            folders[folder] = []
                        folders[folder].append(activity)
                    
                    # Display by folder
                    for folder, activities in folders.items():
                        if folder:
                            st.write(f"#### 📂 Folder: `{folder}`")
                        
                        for i, activity in enumerate(activities):
                            if 'error' in activity:
                                with st.expander(f"❌ Error in {activity.get('source_file', 'Unknown file')}", expanded=False):
                                    st.error(f"**File:** {activity.get('source_file', 'Unknown')}")
                                    st.error(f"**Error:** {activity['error']}")
                            else:
                                with st.expander(f"📄 {activity['source_file']} - {activity['activity_name']}", expanded=False):
                                    st.write(f"**Pipeline:** `{activity.get('pipeline_name', 'Unknown')}`")
                                    st.write(f"**Activity Type:** `{activity['activity_type']}`")
                                    
                                    col1, col2 = st.columns(2)
                                    
                                    with col1:
                                        st.write("**📥 Source:**")
                                        st.write(f"- **Name:** `{activity['source_name']}`")
                                        st.write(f"- **Type:** **{activity['source_type']}**")
                                    
                                    with col2:
                                        st.write("**📤 Sink:**")
                                        st.write(f"- **Name:** `{activity['sink_name']}`")
                                        st.write(f"- **Type:** **{activity['sink_type']}**")
                                    
                                    if activity.get('depends_on'):
                                        st.write(f"**Dependencies:** {', '.join(activity['depends_on'])}")
                    
                    # Download results as JSON
                    if st.button("📥 Download Results as JSON"):
                        results_json = json.dumps(pipeline_data, indent=2)
                        st.download_button(
                            label="Download JSON",
                            data=results_json,
                            file_name="pipeline_analysis_results.json",
                            mime="application/json"
                        )
                        
                else:
                    st.warning("⚠️ No pipeline data found in ZIP file")
                    
            elif uploaded_file.name.endswith('.json'):
                st.write("### 📄 Processing single JSON file...")
                json_content = json.loads(uploaded_file.getvalue().decode("utf-8"))
                pipeline_data = processor.extract_pipeline_data(json_content)
                
                st.write("### 📊 Extracted Pipeline Information:")
                
                for i, activity in enumerate(pipeline_data):
                    if 'error' in activity:
                        st.error(f"❌ Activity {i+1}: {activity['error']}")
                    else:
                        with st.expander(f"📋 Activity {i+1}: {activity['activity_name']}", expanded=True):
                            st.write(f"**Activity Type:** `{activity['activity_type']}`")
                            
                            col1, col2 = st.columns(2)
                            
                            with col1:
                                st.write("**📥 Source:**")
                                st.write(f"- **Name:** `{activity['source_name']}`")
                                st.write(f"- **Type:** **{activity['source_type']}**")
                            
                            with col2:
                                st.write("**📤 Sink:**")
                                st.write(f"- **Name:** `{activity['sink_name']}`")
                                st.write(f"- **Type:** **{activity['sink_type']}**")
                            
                            if activity.get('depends_on'):
                                st.write(f"**Dependencies:** {', '.join(activity['depends_on'])}")
            
            # Legacy document processing for other file types
            elif uploaded_file.type == "application/pdf":
                st.write("### 📄 Processing PDF file...")
                result = processor.process_document(uploaded_file)
                st.write("Processing complete!")
                st.json(result)
                
            elif uploaded_file.type in ["application/vnd.openxmlformats-officedocument.wordprocessingml.document"]:
                st.write("### 📄 Processing DOCX file...")
                result = processor.process_document(uploaded_file)
                st.write("Processing complete!")
                st.json(result)
                
            elif uploaded_file.type == "text/plain":
                st.write("### 📄 Processing TXT file...")
                result = processor.process_document(uploaded_file)
                st.write("Processing complete!")
                st.json(result)
                
            else:
                st.error("❌ Unsupported file type. Please upload a ZIP, JSON, PDF, DOCX, or TXT file.")
                
        except Exception as e:
            st.error(f"❌ Error processing file: {e}")
            st.error("Please make sure your file is valid and properly formatted.")

if __name__ == "__main__":
    main()
