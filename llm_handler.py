import os
import torch
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline
import json
import re

class QwenLLMHandler:
    """Handles Qwen model operations for document analysis"""
    
    def __init__(self, model_name="Qwen/Qwen2.5-7B-Instruct"):
        self.model_name = model_name
        self.tokenizer = None
        self.model = None
        self.pipeline = None
        self._initialize_model()
    
    def _initialize_model(self):
        """Initialize the Qwen model"""
        try:
            print("Loading Qwen model... This may take a few minutes.")
            
            # Load tokenizer
            self.tokenizer = AutoTokenizer.from_pretrained(
                self.model_name,
                trust_remote_code=True
            )
            
            # Load model with appropriate settings for local inference
            self.model = AutoModelForCausalLM.from_pretrained(
                self.model_name,
                torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
                device_map="auto" if torch.cuda.is_available() else None,
                trust_remote_code=True,
                low_cpu_mem_usage=True
            )
            
            # Create text generation pipeline
            self.pipeline = pipeline(
                "text-generation",
                model=self.model,
                tokenizer=self.tokenizer,
                device_map="auto" if torch.cuda.is_available() else None,
                max_new_tokens=2048,
                do_sample=True,
                temperature=0.3,
                top_p=0.9
            )
            
            print("Qwen model loaded successfully!")
            
        except Exception as e:
            print(f"Error initializing Qwen model: {str(e)}")
            # Fallback to a smaller model if the main one fails
            try:
                print("Attempting to load smaller Qwen model...")
                self.model_name = "Qwen/Qwen2.5-3B-Instruct"
                self._initialize_fallback_model()
            except Exception as fallback_error:
                print(f"Fallback model also failed: {str(fallback_error)}")
                raise Exception("Failed to initialize any Qwen model")
    
    def _initialize_fallback_model(self):
        """Initialize smaller fallback model"""
        self.tokenizer = AutoTokenizer.from_pretrained(
            self.model_name,
            trust_remote_code=True
        )
        
        self.model = AutoModelForCausalLM.from_pretrained(
            self.model_name,
            torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32,
            device_map="auto" if torch.cuda.is_available() else None,
            trust_remote_code=True,
            low_cpu_mem_usage=True
        )
        
        self.pipeline = pipeline(
            "text-generation",
            model=self.model,
            tokenizer=self.tokenizer,
            device_map="auto" if torch.cuda.is_available() else None,
            max_new_tokens=1024,
            do_sample=True,
            temperature=0.3,
            top_p=0.9
        )
    
    def analyze_document(self, content, filename=""):
        """Analyze document content for Azure Synapse Pipeline information"""
        try:
            prompt = self._create_analysis_prompt(content, filename)
            
            # Generate response
            messages = [
                {
                    "role": "system",
                    "content": "You are an expert in Azure Synapse Analytics and data pipeline documentation. Analyze the provided content and extract relevant information for Azure Synapse Pipeline documentation."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ]
            
            # Format prompt for the model
            formatted_prompt = self.tokenizer.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=True
            )
            
            # Generate response
            outputs = self.pipeline(
                formatted_prompt,
                max_new_tokens=1024,
                do_sample=True,
                temperature=0.3,
                top_p=0.9,
                pad_token_id=self.tokenizer.eos_token_id
            )
            
            # Extract the generated text
            response = outputs[0]['generated_text']
            # Remove the prompt from the response
            analysis = response[len(formatted_prompt):].strip()
            
            return self._clean_analysis_output(analysis)
            
        except Exception as e:
            print(f"Error during document analysis: {str(e)}")
            return f"Error analyzing {filename}: {str(e)}"
    
    def _create_analysis_prompt(self, content, filename):
        """Create analysis prompt for the document"""
        prompt = f"""
Analyze the following document content for Azure Synapse Pipeline information. Extract and identify:

1. Data Flow Names and Pipeline Names
2. Source Systems (databases, files, APIs, etc.)
3. Sink/Destination Systems
4. Data Transformations and Business Rules
5. Dependencies (upstream and downstream)
6. Performance considerations
7. Error handling or data quality rules

Document: {filename}
Content:
{content[:3000]}  # Limit content to avoid token limits

Please provide a structured analysis focusing on Azure Synapse Pipeline components and data flow information.
"""
        return prompt
    
    def _clean_analysis_output(self, analysis):
        """Clean and format the analysis output"""
        # Remove any unwanted characters or formatting
        analysis = re.sub(r'\n{3,}', '\n\n', analysis)  # Reduce multiple newlines
        analysis = analysis.strip()
        
        # Ensure the analysis is not empty
        if not analysis or len(analysis) < 10:
            return "No significant pipeline information found in this document."
        
        return analysis
    
    def synthesize_pipeline_info(self, analysis_results):
        """Synthesize information from multiple analyzed documents"""
        try:
            # Combine all analysis results
            combined_content = "\n\n=== DOCUMENT ANALYSIS RESULTS ===\n\n"
            
            for result in analysis_results:
                combined_content += f"File: {result['filename']}\n"
                combined_content += f"Analysis: {result['analysis']}\n"
                combined_content += "="*50 + "\n\n"
            
            # Create synthesis prompt
            prompt = f"""
Based on the following document analyses, create a comprehensive Azure Synapse Pipeline documentation structure.

Synthesize the information to populate these sections:
1. Data Flow Name - Identify the main pipeline/data flow name
2. Sources and Sinks - List all data sources and destinations
3. Data Flow Logic & Business Rules - Key transformations and rules
4. Dependencies - Upstream and downstream systems
5. Performance & Maintenance - Any performance notes or considerations

Analysis Results:
{combined_content[:4000]}  # Limit to avoid token limits

Please provide a structured synthesis that can be used to populate an Azure Synapse Pipeline documentation template.
"""

            messages = [
                {
                    "role": "system",
                    "content": "You are an expert Azure Synapse Analytics consultant. Synthesize multiple document analyses into comprehensive pipeline documentation."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ]
            
            formatted_prompt = self.tokenizer.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=True
            )
            
            outputs = self.pipeline(
                formatted_prompt,
                max_new_tokens=1536,
                do_sample=True,
                temperature=0.2,
                top_p=0.9,
                pad_token_id=self.tokenizer.eos_token_id
            )
            
            response = outputs[0]['generated_text']
            synthesis = response[len(formatted_prompt):].strip()
            
            return {
                'synthesis': synthesis,
                'individual_analyses': analysis_results,
                'source_files': [result['filename'] for result in analysis_results]
            }
            
        except Exception as e:
            print(f"Error during synthesis: {str(e)}")
            return {
                'synthesis': f"Error during synthesis: {str(e)}",
                'individual_analyses': analysis_results,
                'source_files': [result['filename'] for result in analysis_results]
            }
    
    def extract_specific_info(self, content, info_type):
        """Extract specific type of information from content"""
        try:
            prompts = {
                'data_flow_name': "Extract the data flow or pipeline name from this content. If multiple names exist, list them all.",
                'sources': "Identify all data sources mentioned in this content (databases, files, APIs, etc.).",
                'sinks': "Identify all data destinations or sinks mentioned in this content.",
                'transformations': "List all data transformations and business rules mentioned in this content.",
                'dependencies': "Identify upstream and downstream dependencies mentioned in this content."
            }
            
            if info_type not in prompts:
                return "Unknown information type requested."
            
            prompt = f"{prompts[info_type]}\n\nContent:\n{content[:2000]}"
            
            messages = [
                {
                    "role": "system",
                    "content": "You are an expert in data pipeline analysis. Provide concise and accurate information extraction."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ]
            
            formatted_prompt = self.tokenizer.apply_chat_template(
                messages,
                tokenize=False,
                add_generation_prompt=True
            )
            
            outputs = self.pipeline(
                formatted_prompt,
                max_new_tokens=512,
                do_sample=True,
                temperature=0.2,
                top_p=0.9,
                pad_token_id=self.tokenizer.eos_token_id
            )
            
            response = outputs[0]['generated_text']
            extraction = response[len(formatted_prompt):].strip()
            
            return extraction
            
        except Exception as e:
            return f"Error extracting {info_type}: {str(e)}"
