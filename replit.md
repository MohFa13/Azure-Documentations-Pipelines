# DocuFillGenie - Azure Synapse Pipeline Documentation Generator

## Overview

DocuFillGenie is a Streamlit-based web application that automates the generation of Azure Synapse Pipeline documentation. The system processes uploaded documents (Word, PDF, Excel, text files) and uses AI-powered analysis to extract relevant information about data pipelines, transformations, and business rules. It then generates standardized documentation following Azure Synapse Pipeline documentation templates.

The application is designed to streamline the documentation process for data engineers and analysts working with Azure Synapse pipelines by automatically extracting key information from existing documents and organizing it into a structured, professional format.

## User Preferences

Preferred communication style: Simple, everyday language.

## System Architecture

### Frontend Architecture
- **Framework**: Streamlit web framework for rapid UI development
- **Layout**: Wide layout with expandable sidebar for better user experience
- **Session Management**: Uses Streamlit's session state to maintain component instances and processing status across user interactions
- **File Upload**: Multi-file upload support with drag-and-drop interface

### Backend Processing Pipeline
- **Document Processing**: Multi-format document processor supporting TXT, DOCX, PDF, Excel, and CSV files
- **Content Extraction**: Format-specific extraction methods using libraries like python-docx, PyPDF2, and pandas
- **AI Analysis**: Dual approach with both full LLM integration (Qwen) and simplified rule-based analysis as fallback
- **Template Generation**: Automated Word document generation using python-docx with predefined Azure Synapse Pipeline templates

### LLM Integration Strategy
- **Primary Handler**: Qwen 2.5-7B-Instruct model with local inference capabilities
- **Fallback Handler**: Rule-based pattern matching system for environments where full LLM deployment isn't feasible
- **Model Management**: Automatic GPU/CPU detection and memory-optimized loading

### Document Generation System
- **Template Structure**: Standardized Azure Synapse Pipeline documentation sections including General Information, Sources/Sinks, Data Flow Logic, Dependencies, and Performance considerations
- **Content Synthesis**: Intelligent merging of information from multiple source documents
- **Output Format**: Professional Word documents with proper formatting, tables, and placeholder sections for screenshots

### File Processing Architecture
- **File Validation**: Extension and MIME type checking with optional magic library integration
- **Temporary Storage**: Secure temporary file handling for uploaded content processing
- **Batch Processing**: Support for processing multiple documents simultaneously

## External Dependencies

### Core Python Libraries
- **streamlit**: Web application framework for the user interface
- **transformers**: Hugging Face library for LLM model loading and inference
- **torch**: PyTorch for deep learning model operations
- **python-docx**: Word document creation and manipulation
- **PyPDF2**: PDF document text extraction
- **pandas**: Excel and CSV file processing
- **pathlib**: Modern file path handling

### Optional Dependencies
- **python-magic**: Enhanced file type detection (with fallback to extension-based detection)
- **chardet**: Character encoding detection for text files

### Model Dependencies
- **Qwen/Qwen2.5-7B-Instruct**: Primary language model for document analysis and content synthesis
- **AutoTokenizer/AutoModelForCausalLM**: Hugging Face model loading infrastructure

### System Requirements
- **Hardware**: GPU support preferred but CPU fallback available
- **Memory**: Sufficient RAM for loading 7B parameter model (approximately 14GB+ recommended)
- **Storage**: Temporary file storage for document processing and model caching