"""
Azure Synapse Pipeline Documentation Template
This module contains the template structure and helper functions
for generating Azure Synapse Pipeline documentation.
"""

class AzureTemplateStructure:
    """Defines the structure of Azure Synapse Pipeline documentation"""
    
    @staticmethod
    def get_template_sections():
        """Get the standard template sections"""
        return {
            "general_information": {
                "title": "1. General Information",
                "subsections": [
                    "data_flow_name",
                    "data_flow_screenshot"
                ]
            },
            "sources_and_sinks": {
                "title": "2. Source(s) and Sink(s)",
                "fields": [
                    "source_sink_name",
                    "type",
                    "location", 
                    "format",
                    "description_notes"
                ]
            },
            "data_flow_logic": {
                "title": "3. Data Flow Logic & Business Rules",
                "subsections": [
                    "step_by_step_breakdown",
                    "key_transformations_business_rules"
                ]
            },
            "dependencies": {
                "title": "4. Dependencies & Related Assets",
                "subsections": [
                    "upstream_dependencies",
                    "downstream_dependencies"
                ]
            },
            "performance_maintenance": {
                "title": "6. Performance & Maintenance",
                "subsections": [
                    "integration_runtime",
                    "known_issues_considerations",
                    "performance_tuning_notes"
                ]
            },
            "change_log": {
                "title": "7. Change Log",
                "fields": [
                    "version",
                    "date",
                    "author",
                    "changes_made"
                ]
            }
        }
    
    @staticmethod
    def get_field_descriptions():
        """Get descriptions for each field"""
        return {
            "data_flow_name": "Provide the official name of the data flow as it appears in Azure Synapse Analytics. This should be specific enough to distinguish it from other data flows in your environment. Use your organization's naming conventions to ensure consistency.",
            
            "step_by_step_breakdown": "Document complex transformation steps in the data flow if its used, no need to capture simple transformation like joins / unions etc.,",
            
            "key_transformations_business_rules": "Expand upon each rule, describing its business impact, rationale, and technical implementation.",
            
            "upstream_dependencies": "Identify the ETL pipelines, data ingestion processes, or external systems supplying source data to the data flow. Document their names, owners, schedules, and any critical SLAs (Service Level Agreements) or data freshness requirements.",
            
            "downstream_dependencies": "List reports, dashboards (e.g., Power BI), machine learning models, or other business processes that consume the output. Clarify how changes to this data flow might impact these dependencies, and note stakeholders for coordination.",
            
            "integration_runtime": "Specify which Azure Integration Runtime is used (auto or self-hosted), compute size, and any configuration details that affect execution or scalability.",
            
            "known_issues_considerations": "Document any limitations, data quality concerns, error patterns, or historical challenges encountered in this data flow. Include troubleshooting tips if available.",
            
            "performance_tuning_notes": 'Record optimizations made to improve throughput, latency, or reliability. For example: "Increased parallelism by scaling out to 16 cores," or "Partitioned source files by month to reduce load time."'
        }
    
    @staticmethod
    def get_example_transformations():
        """Get example transformation patterns"""
        return [
            {
                "name": "Filter_InvalidRows",
                "description": "Exclude any records where the sales amount is zero or negative, as these do not represent valid transactions."
            },
            {
                "name": "DerivedColumn_GrossMargin",
                "description": "Compute gross margin for each transaction using the formula (sale_price - cost) / sale_price. Validate that inputs are not null."
            }
        ]
    
    @staticmethod
    def get_example_business_rules():
        """Get example business rule patterns"""
        return [
            "Exclude all transactions dated before January 1, 2023, to comply with regulatory reporting requirements.",
            "Apply currency conversion to all sales involving foreign currencies, referencing the exchange rate from the CurrencyLookup table for the transaction date."
        ]
    
    @staticmethod
    def get_default_change_log():
        """Get default change log structure"""
        return [
            {
                "version": "1.0",
                "date": "",
                "author": "",
                "changes_made": "Initial version."
            },
            {
                "version": "1.1", 
                "date": "",
                "author": "",
                "changes_made": "Added a new filter for invalid dates."
            }
        ]
    
    @staticmethod
    def validate_template_data(data):
        """Validate that all required template sections are present"""
        required_sections = [
            "general_information",
            "sources_and_sinks", 
            "data_flow_logic",
            "dependencies",
            "performance_maintenance",
            "change_log"
        ]
        
        missing_sections = []
        for section in required_sections:
            if section not in data:
                missing_sections.append(section)
        
        return len(missing_sections) == 0, missing_sections

class AzureTemplateHelpers:
    """Helper functions for working with Azure template data"""
    
    @staticmethod
    def extract_pipeline_names(content):
        """Extract potential pipeline names from content"""
        import re
        
        # Common patterns for pipeline names
        patterns = [
            r'pipeline[:\s]+([a-zA-Z0-9_\-\s]+)',
            r'data\s*flow[:\s]+([a-zA-Z0-9_\-\s]+)',
            r'dataflow[:\s]+([a-zA-Z0-9_\-\s]+)',
            r'([a-zA-Z0-9_\-]+)\s*pipeline',
            r'([a-zA-Z0-9_\-]+)\s*data\s*flow'
        ]
        
        names = []
        content_lower = content.lower()
        
        for pattern in patterns:
            matches = re.findall(pattern, content_lower, re.IGNORECASE)
            names.extend([match.strip() for match in matches if match.strip()])
        
        # Remove duplicates and return
        return list(set(names))
    
    @staticmethod
    def extract_data_sources(content):
        """Extract potential data sources from content"""
        import re
        
        source_patterns = [
            r'source[:\s]+([a-zA-Z0-9_\-\.\s]+)',
            r'from[:\s]+([a-zA-Z0-9_\-\.\s]+)',
            r'database[:\s]+([a-zA-Z0-9_\-\.\s]+)',
            r'table[:\s]+([a-zA-Z0-9_\-\.\s]+)',
            r'file[:\s]+([a-zA-Z0-9_\-\.\s]+)'
        ]
        
        sources = []
        content_lower = content.lower()
        
        for pattern in source_patterns:
            matches = re.findall(pattern, content_lower, re.IGNORECASE)
            sources.extend([match.strip() for match in matches if match.strip()])
        
        return list(set(sources))
    
    @staticmethod
    def extract_transformations(content):
        """Extract transformation steps from content"""
        import re
        
        transform_patterns = [
            r'transform[:\s]+([a-zA-Z0-9_\-\s]+)',
            r'filter[:\s]+([a-zA-Z0-9_\-\s]+)',
            r'join[:\s]+([a-zA-Z0-9_\-\s]+)',
            r'aggregate[:\s]+([a-zA-Z0-9_\-\s]+)',
            r'derive[d]?\s*column[:\s]+([a-zA-Z0-9_\-\s]+)'
        ]
        
        transformations = []
        content_lower = content.lower()
        
        for pattern in transform_patterns:
            matches = re.findall(pattern, content_lower, re.IGNORECASE)
            transformations.extend([match.strip() for match in matches if match.strip()])
        
        return list(set(transformations))
    
    @staticmethod
    def format_section_content(content, max_length=1000):
        """Format content for inclusion in template sections"""
        if not content:
            return "No information available from source documents."
        
        # Clean up content
        content = content.strip()
        
        # Truncate if too long
        if len(content) > max_length:
            content = content[:max_length] + "..."
        
        return content
    
    @staticmethod
    def merge_analysis_results(analysis_results):
        """Merge multiple analysis results into cohesive sections"""
        merged = {
            "pipeline_names": [],
            "sources": [],
            "sinks": [],
            "transformations": [],
            "business_rules": [],
            "dependencies": [],
            "performance_notes": []
        }
        
        for result in analysis_results:
            content = result.get('content', '')
            analysis = result.get('analysis', '')
            
            # Extract information from content and analysis
            merged["pipeline_names"].extend(AzureTemplateHelpers.extract_pipeline_names(content + " " + analysis))
            merged["sources"].extend(AzureTemplateHelpers.extract_data_sources(content + " " + analysis))
            merged["transformations"].extend(AzureTemplateHelpers.extract_transformations(content + " " + analysis))
        
        # Remove duplicates
        for key in merged:
            if isinstance(merged[key], list):
                merged[key] = list(set(merged[key]))
        
        return merged
