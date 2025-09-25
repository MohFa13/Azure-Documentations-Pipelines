from QwenDocGenie.document_processor import fix_reference_names, extract_sources_from_docx

import streamlit as st
import os
import tempfile
import zipfile
from pathlib import Path
import pandas as pd
from document_processor import DocumentProcessor
from llm_handler_simple import SimpleLLMHandler as QwenLLMHandler
from template_generator import AzureTemplateGenerator
from file_utils import FileUtils
import traceback

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
    """Main application function"""
    st.title("📄 DocuFillGenie")
    st.markdown("### AI-Powered Azure Synapse Pipeline Documentation Generator")
    
    # Initialize components
    if not initialize_components():
        return
    
    # Sidebar for file upload
    with st.sidebar:
        st.header("📁 File Upload")
        uploaded_files = st.file_uploader(
            "Upload documents or zip files",
            type=['zip', 'docx', 'doc', 'txt', 'pdf', 'xlsx', 'xls', 'csv', 'json'],
            accept_multiple_files=True,
            help="Upload zip files containing pipeline documentation or individual documents"
        )
        
        st.divider()
        
        # Optional screenshot upload section
        st.subheader("📷 Data Flow Screenshots (Optional)")
        st.markdown("Upload screenshots of your data flow diagrams to include in the documentation")
        
        uploaded_screenshots = st.file_uploader(
            "Upload dataflow screenshots",
            type=['png', 'jpg', 'jpeg', 'gif', 'bmp'],
            accept_multiple_files=True,
            help="Optional: Upload screenshots of your data flow diagrams"
        )
        
        if uploaded_screenshots:
            st.write(f"📷 {len(uploaded_screenshots)} screenshot(s) uploaded")
            for screenshot in uploaded_screenshots:
                st.write(f"- {screenshot.name}")
        
        if uploaded_files:
            st.write(f"📄 {len(uploaded_files)} document(s) uploaded")
            for file in uploaded_files:
                st.write(f"- {file.name}")
    
    # Main content area
    if uploaded_files:
        col1, col2 = st.columns([3, 1])
        
        with col2:
            process_button = st.button("🚀 Process Documents", type="primary", use_container_width=True)
        
        if process_button:
            st.session_state.processing_complete = False
            st.session_state.doc_path = None
            
            # Step 1: Process uploaded files
            st.subheader("📂 File Processing")
            extracted_data = process_uploaded_files(uploaded_files)
            
            if extracted_data:
                st.success(f"Successfully extracted content from {len(extracted_data)} files")
                
                # Display extracted files summary
                with st.expander("📋 Extracted Files Summary"):
                    df = pd.DataFrame([
                        {
                            'Filename': data['filename'],
                            'Source': data['source'],
                            'Content Length': len(data['content'])
                        }
                        for data in extracted_data
                    ])
                    st.dataframe(df, use_container_width=True)
                
                # Step 2: Analyze with LLM
                analysis_results = analyze_with_llm(extracted_data)
                
                if analysis_results:
                    # Display analysis summary
                    with st.expander("🔍 AI Analysis Results"):
                        for result in analysis_results:
                            st.write(f"**{result['filename']}**")
                            st.write(result['analysis'])
                            st.divider()
                    
                    # Step 3: Generate documentation
                    doc_path = generate_documentation(analysis_results, uploaded_screenshots)
                    
                    if doc_path:
                        st.balloons()
            else:
                st.error("No valid content could be extracted from the uploaded files.")
    
    # Download section
    if st.session_state.processing_complete and st.session_state.doc_path:
        st.subheader("⬇️ Download Generated Documentation")
        
        try:
            with open(st.session_state.doc_path, 'rb') as file:
                st.download_button(
                    label="📄 Download Azure Synapse Pipeline Documentation",
                    data=file.read(),
                    file_name="Azure_Synapse_Pipeline_Documentation.docx",
                    mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
                    use_container_width=True
                )
        except Exception as e:
            st.error(f"Error preparing download: {str(e)}")
    
    # Instructions section
    if not uploaded_files:
        st.markdown("## 🚀 Getting Started")
        st.markdown("""
        1. **Upload your files** using the sidebar file uploader
        2. **Supported formats**: ZIP files, Word documents (.docx, .doc), PDF, Excel files, CSV, and text files
        3. **Click 'Process Documents'** to start the AI analysis
        4. **Download** your generated Azure Synapse Pipeline documentation
        
        ### 📋 What this tool does:
        - Extracts content from your uploaded documents
        - Uses advanced Qwen AI model to analyze pipeline information
        - Generates professional Azure Synapse Pipeline documentation
        - Outputs a properly formatted Word document following Azure standards
        """)
        
        st.markdown("### 📄 Example Files")
        st.markdown("""
        Upload zip files containing:
        - Pipeline configuration files
        - Data flow documentation
        - Source and sink specifications
        - Business rules and transformation logic
        """)

if __name__ == "__main__":
    main()
