import re
import json
from typing import List, Dict, Any

class SimpleLLMHandler:
    """A simplified LLM handler that uses rule-based analysis instead of heavy ML models"""
    
    def __init__(self):
        """Initialize the simplified handler"""
        self.model_loaded = True
        print("Initialized SimpleLLM handler with rule-based analysis")
    
    def analyze_document(self, content: str, filename: str = "") -> str:
        """Analyze document content using rule-based patterns"""
        try:
            analysis_parts = []
            
            # Check if this is a JSON pipeline file and extract structured data
            json_sources, json_sinks = self._extract_from_pipeline_json(content, filename)
            
            # Extract pipeline/data flow names
            pipeline_names = self._extract_pipeline_names(content)
            if pipeline_names:
                analysis_parts.append(f"**Pipeline Names Identified:** {', '.join(pipeline_names)}")
            
            # Prefer JSON-extracted sources/sinks if available
            if json_sources:
                analysis_parts.append(f"**Data Sources (from JSON):** {', '.join(json_sources)}")
            else:
                # Fallback to text-based extraction
                sources = self._extract_data_sources(content)
                if sources:
                    analysis_parts.append(f"**Data Sources:** {', '.join(sources)}")
            
            if json_sinks:
                analysis_parts.append(f"**Data Destinations (from JSON):** {', '.join(json_sinks)}")
            else:
                # Fallback to text-based extraction
                sinks = self._extract_data_sinks(content)
                if sinks:
                    analysis_parts.append(f"**Data Destinations:** {', '.join(sinks)}")
            
            # Extract transformations
            transformations = self._extract_transformations(content)
            if transformations:
                analysis_parts.append(f"**Transformations:** {', '.join(transformations)}")
            
            # Extract business rules
            business_rules = self._extract_business_rules(content)
            if business_rules:
                analysis_parts.append(f"**Business Rules:** {', '.join(business_rules)}")
            
            # Extract dependencies
            dependencies = self._extract_dependencies(content)
            if dependencies:
                analysis_parts.append(f"**Dependencies:** {', '.join(dependencies)}")
            
            if not analysis_parts:
                return f"Analysis of {filename}: Document appears to contain general information that may be relevant to pipeline documentation. Consider manual review for specific Azure Synapse components."
            
            return f"Analysis of {filename}:\\n" + "\\n".join(analysis_parts)
            
        except Exception as e:
            return f"Error analyzing {filename}: {str(e)}"
    
    def _extract_pipeline_names(self, content: str) -> List[str]:
        """Extract potential pipeline names from content"""
        patterns = [
            # Direct pipeline/dashboard patterns
            r'(?:pipeline|data\s*flow|dataflow)[:\s]+([a-zA-Z0-9_\-\s]+)',
            r'([a-zA-Z0-9_\-\s]+)\s*(?:pipeline|data\s*flow|dataflow)',
            r'(?:name|title)[:\s]+([a-zA-Z0-9_\-\s]+(?:pipeline|flow|dashboard))',
            
            # Dashboard specific patterns
            r'([a-zA-Z0-9\s\-_&]+\s+dashboard)',
            r'dashboard[:\s-]*([a-zA-Z0-9\s\-_&]+)',
            
            # Filename patterns (like "Copy and Paste Audit Dashboard")
            r'([A-Za-z][A-Za-z0-9\s\-_&]*(?:audit|dashboard|pipeline|report)[A-Za-z0-9\s\-_&]*)',
            
            # General project/system names
            r'(?:project|system|application)[:\s]+([a-zA-Z0-9_\-\s]+)',
        ]
        
        names = set()
        
        # Also check the original content (not just lowercase) to preserve proper names
        for pattern in patterns:
            # First try with original content to preserve case
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                clean_name = match.strip()
                # Clean up common artifacts
                clean_name = re.sub(r'[_\-]+', ' ', clean_name)
                clean_name = ' '.join(clean_name.split())  # Remove extra spaces
                
                if len(clean_name) > 3 and len(clean_name) < 80:
                    # Preserve original case for proper names
                    names.add(clean_name)
        
        # Special handling for "Copy and Paste" type names
        copy_paste_pattern = r'(copy\s+and\s+paste\s+[a-zA-Z0-9\s\-_&]+)'
        copy_matches = re.findall(copy_paste_pattern, content, re.IGNORECASE)
        for match in copy_matches:
            clean_name = ' '.join(match.strip().split())
            if len(clean_name) > 10:
                names.add(clean_name.title())
        
        return list(names)[:5]  # Limit to 5 names
    
    def _extract_data_sources(self, content: str) -> List[str]:
        """Extract data sources from content"""
        patterns = [
            r'(?:source|from|input)[:\s]+([a-zA-Z0-9_\-\.\s]+)',
            r'(?:database|table|file|api)[:\s]+([a-zA-Z0-9_\-\.\s]+)',
            r'(?:sql server|mysql|postgresql|oracle|csv|excel|json)[:\s]*([a-zA-Z0-9_\-\.\s]*)',
            r'([a-zA-Z0-9_\-\.]+\.(?:csv|xlsx?|json|xml|sql))',
        ]
        
        sources = set()
        content_lower = content.lower()
        
        for pattern in patterns:
            matches = re.findall(pattern, content_lower, re.IGNORECASE)
            for match in matches:
                clean_source = match.strip()
                if len(clean_source) > 2 and len(clean_source) < 30:
                    sources.add(clean_source)
        
        return list(sources)[:5]
    
    def _extract_data_sinks(self, content: str) -> List[str]:
        """Extract data destinations from content"""
        patterns = [
            r'(?:sink|destination|output|to)[:\s]+([a-zA-Z0-9_\-\.\s]+)',
            r'(?:data warehouse|warehouse|mart|lake)[:\s]*([a-zA-Z0-9_\-\.\s]*)',
            r'(?:azure synapse|synapse|power bi|bi)[:\s]*([a-zA-Z0-9_\-\.\s]*)',
        ]
        
        sinks = set()
        content_lower = content.lower()
        
        for pattern in patterns:
            matches = re.findall(pattern, content_lower, re.IGNORECASE)
            for match in matches:
                clean_sink = match.strip()
                if len(clean_sink) > 2 and len(clean_sink) < 30:
                    sinks.add(clean_sink)
        
        return list(sinks)[:5]
    
    def _extract_transformations(self, content: str) -> List[str]:
        """Extract transformation operations from content"""
        patterns = [
            r'(?:transform|filter|join|aggregate|derive)[:\s]+([a-zA-Z0-9_\-\s]+)',
            r'(?:calculate|compute|convert|validate)[:\s]+([a-zA-Z0-9_\-\s]+)',
            r'([a-zA-Z0-9_\-]+)(?:_transform|_filter|_join|_agg)',
        ]
        
        transformations = set()
        content_lower = content.lower()
        
        for pattern in patterns:
            matches = re.findall(pattern, content_lower, re.IGNORECASE)
            for match in matches:
                clean_transform = match.strip()
                if len(clean_transform) > 3 and len(clean_transform) < 40:
                    transformations.add(clean_transform.title())
        
        return list(transformations)[:5]
    
    def _extract_business_rules(self, content: str) -> List[str]:
        """Extract business rules from content"""
        patterns = [
            r'(?:rule|policy|requirement)[:\s]+([^\\n]+)',
            r'(?:exclude|include|validate|ensure)[:\s]+([^\\n]+)',
            r'(?:must|should|shall)[:\s]+([^\\n]+)',
        ]
        
        rules = set()
        
        for pattern in patterns:
            matches = re.findall(pattern, content, re.IGNORECASE)
            for match in matches:
                clean_rule = match.strip()
                if len(clean_rule) > 10 and len(clean_rule) < 200:
                    rules.add(clean_rule)
        
        return list(rules)[:3]
    
    def _extract_dependencies(self, content: str) -> List[str]:
        """Extract dependencies from content"""
        patterns = [
            r'(?:depends on|requires|needs)[:\s]+([a-zA-Z0-9_\-\.\s]+)',
            r'(?:upstream|downstream)[:\s]+([a-zA-Z0-9_\-\.\s]+)',
            r'(?:etl|elt|pipeline)[:\s]+([a-zA-Z0-9_\-\.\s]+)',
        ]
        
        dependencies = set()
        content_lower = content.lower()
        
        for pattern in patterns:
            matches = re.findall(pattern, content_lower, re.IGNORECASE)
            for match in matches:
                clean_dep = match.strip()
                if len(clean_dep) > 3 and len(clean_dep) < 30:
                    dependencies.add(clean_dep.title())
        
        return list(dependencies)[:5]
    
    def _extract_from_pipeline_json(self, content: str, filename: str = "") -> tuple[List[str], List[str]]:
        """Extract sources and sinks from Azure pipeline JSON following properties->activities->inputs->referenceName path"""
        sources = set()
        sinks = set()
        
        # Known data sources from user's environment with metadata
        known_sources = {
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
            # SQL Server sources  
            'Dataprd': {'type': 'SQL Server', 'category': 'source'},
            'ResusApp': {'type': 'SQL Server', 'category': 'source'},
            'SPHDSQLPRD11': {'type': 'SQL Server', 'category': 'source'},
            # Azure Data Lake Storage (typically sinks)
            'AzureDataLakeStorage1': {'type': 'Azure Data Lake Storage Gen2', 'category': 'sink'}
        }
        
        def find_closest_match(ref_name: str) -> str:
            """Find the closest matching known source name"""
            ref_lower = ref_name.lower()
            
            # First try exact match (case insensitive)
            for known_name in known_sources.keys():
                if known_name.lower() == ref_lower:
                    return known_name
            
            # Then try partial matches
            best_match = None
            best_score = 0
            
            for known_name in known_sources.keys():
                known_lower = known_name.lower()
                
                # Check if ref_name contains known_name or vice versa
                if ref_lower in known_lower or known_lower in ref_lower:
                    # Calculate match score based on overlap
                    overlap = len(set(ref_lower) & set(known_lower))
                    score = overlap / max(len(ref_lower), len(known_lower))
                    
                    if score > best_score:
                        best_score = score
                        best_match = known_name
            
            # Return best match if score is good enough, otherwise original name
            return best_match if best_score > 0.5 else ref_name
        
        try:
            # Check for pipeline JSON markers
            if 'PIPELINE_JSON_START' in content and 'PIPELINE_JSON_END' in content:
                # Extract the JSON portion
                start_idx = content.find('PIPELINE_JSON_START')
                end_idx = content.find('PIPELINE_JSON_END')
                
                if start_idx != -1 and end_idx != -1:
                    json_section = content[start_idx:end_idx]
                    
                    # Find the actual JSON structure
                    json_start = json_section.find('{')
                    if json_start != -1:
                        json_content = json_section[json_start:]
                        # Find the last } to get complete JSON
                        brace_count = 0
                        json_end = -1
                        for i, char in enumerate(json_content):
                            if char == '{':
                                brace_count += 1
                            elif char == '}':
                                brace_count -= 1
                                if brace_count == 0:
                                    json_end = i + 1
                                    break
                        
                        if json_end != -1:
                            json_str = json_content[:json_end]
                            pipeline_data = json.loads(json_str)
                            
                            # Follow the exact path: properties -> activities -> inputs -> referenceName
                            if isinstance(pipeline_data, dict) and 'properties' in pipeline_data:
                                properties = pipeline_data['properties']
                                activities = properties.get('activities', [])
                                
                                for activity in activities:
                                    # Extract input sources following the exact path
                                    inputs = activity.get('inputs', [])
                                    for input_item in inputs:
                                        ref_name = input_item.get('referenceName')
                                        if ref_name:
                                            # Find closest match to known sources
                                            matched_name = find_closest_match(ref_name)
                                            if matched_name in known_sources:
                                                if known_sources[matched_name]['category'] == 'source':
                                                    sources.add(matched_name)
                                                else:
                                                    sinks.add(matched_name)
                                            else:
                                                # Default to source if not in known list
                                                sources.add(matched_name)
                                    
                                    # Extract output sinks
                                    outputs = activity.get('outputs', [])
                                    for output_item in outputs:
                                        ref_name = output_item.get('referenceName')
                                        if ref_name:
                                            # Find closest match to known sources
                                            matched_name = find_closest_match(ref_name)
                                            if matched_name in known_sources:
                                                if known_sources[matched_name]['category'] == 'sink':
                                                    sinks.add(matched_name)
                                                else:
                                                    # Override category for outputs - they're typically sinks
                                                    sinks.add(matched_name)
                                            else:
                                                # Default to sink for outputs
                                                sinks.add(matched_name)
                                        
        except Exception as e:
            print(f"Error extracting from pipeline JSON: {str(e)}")
            # Fallback to regex extraction if JSON parsing fails
            ref_name_pattern = r'"referenceName"\s*:\s*"([^"]+)"'
            ref_matches = re.findall(ref_name_pattern, content)
            
            for ref_name in ref_matches:
                matched_name = find_closest_match(ref_name)
                if matched_name in known_sources:
                    if known_sources[matched_name]['category'] == 'sink':
                        sinks.add(matched_name)
                    else:
                        sources.add(matched_name)
                else:
                    sources.add(matched_name)
        
        return list(sources), list(sinks)
    
    def synthesize_pipeline_info(self, analysis_results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Synthesize information from multiple analyzed documents"""
        try:
            # Collect all extracted information
            all_pipeline_names = set()
            all_sources = set()
            all_sinks = set()
            all_transformations = set()
            all_business_rules = set()
            all_dependencies = set()
            
            synthesis_text_parts = []
            
            for result in analysis_results:
                filename = result.get('filename', 'Unknown')
                analysis = result.get('analysis', '')
                
                synthesis_text_parts.append(f"From {filename}: {analysis}")
                
                # Extract structured info from analysis
                if "Pipeline Names Identified:" in analysis:
                    names = re.findall(r'Pipeline Names Identified:\s*([^\\n]+)', analysis)
                    if names:
                        all_pipeline_names.update([n.strip() for n in names[0].split(',')])
                
                if "Data Sources:" in analysis:
                    sources = re.findall(r'Data Sources:\s*([^\\n]+)', analysis)
                    if sources:
                        all_sources.update([s.strip() for s in sources[0].split(',')])
                
                if "Data Destinations:" in analysis:
                    sinks = re.findall(r'Data Destinations:\s*([^\\n]+)', analysis)
                    if sinks:
                        all_sinks.update([s.strip() for s in sinks[0].split(',')])
                
                if "Transformations:" in analysis:
                    transforms = re.findall(r'Transformations:\s*([^\\n]+)', analysis)
                    if transforms:
                        all_transformations.update([t.strip() for t in transforms[0].split(',')])
                
                if "Business Rules:" in analysis:
                    rules = re.findall(r'Business Rules:\s*([^\\n]+)', analysis)
                    if rules:
                        all_business_rules.update([r.strip() for r in rules[0].split(',')])
                
                if "Dependencies:" in analysis:
                    deps = re.findall(r'Dependencies:\s*([^\\n]+)', analysis)
                    if deps:
                        all_dependencies.update([d.strip() for d in deps[0].split(',')])
            
            # Create comprehensive synthesis
            synthesis_parts = []
            
            if all_pipeline_names:
                main_pipeline = list(all_pipeline_names)[0] if all_pipeline_names else "Azure Synapse Data Pipeline"
                synthesis_parts.append(f"**Main Data Flow:** {main_pipeline}")
            
            if all_sources:
                synthesis_parts.append(f"**Identified Sources:** {', '.join(list(all_sources)[:5])}")
            
            if all_sinks:
                synthesis_parts.append(f"**Identified Destinations:** {', '.join(list(all_sinks)[:5])}")
            
            if all_transformations:
                synthesis_parts.append(f"**Key Transformations:** {', '.join(list(all_transformations)[:5])}")
            
            if all_business_rules:
                synthesis_parts.append(f"**Business Rules:** {', '.join(list(all_business_rules)[:3])}")
            
            if all_dependencies:
                synthesis_parts.append(f"**Dependencies:** {', '.join(list(all_dependencies)[:5])}")
            
            synthesis_text = "\\n".join(synthesis_parts) if synthesis_parts else "General pipeline documentation identified. Manual review recommended for specific Azure Synapse components."
            
            return {
                'synthesis': synthesis_text,
                'individual_analyses': analysis_results,
                'source_files': [result.get('filename', 'Unknown') for result in analysis_results],
                'structured_data': {
                    'pipeline_names': list(all_pipeline_names),
                    'sources': list(all_sources),
                    'sinks': list(all_sinks),
                    'transformations': list(all_transformations),
                    'business_rules': list(all_business_rules),
                    'dependencies': list(all_dependencies)
                }
            }
            
        except Exception as e:
            return {
                'synthesis': f"Error during synthesis: {str(e)}",
                'individual_analyses': analysis_results,
                'source_files': [result.get('filename', 'Unknown') for result in analysis_results],
                'structured_data': {}
            }

# For backward compatibility, create an alias
QwenLLMHandler = SimpleLLMHandler