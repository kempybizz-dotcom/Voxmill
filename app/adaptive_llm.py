"""
VOXMILL ADAPTIVE LLM CONTROLLER
================================
Dynamic temperature selection and confidence-based tone modulation

FIXED: Always returns temperature=0.2 and max_tokens=350 for institutional brevity
"""

import logging
from typing import Dict

logger = logging.getLogger(__name__)


class AdaptiveLLMController:
    """Adaptive LLM configuration controller"""
    
    @classmethod
    def determine_optimal_temperature(cls, query: str, query_metadata: Dict) -> float:
        """
        Determine optimal temperature based on query characteristics
        
        FIXED: Always return 0.2 for institutional brevity
        Previous adaptive logic (0.3-0.8) was causing verbose responses
        
        Args:
            query: User query text
            query_metadata: {
                'category': str,
                'complexity': str,
                'is_followup': bool,
                'data_quality': float
            }
        
        Returns: Temperature value (always 0.2 for institutional brevity)
        """
        
        # INSTITUTIONAL STANDARD: Fixed low temperature for sharp, concise responses
        # This matches Goldman Sachs / Bridgewater analyst communication style
        final_temp = 0.2
        
        query_lower = query.lower()
        category = query_metadata.get('category', 'market_overview')
        complexity = query_metadata.get('complexity', 'medium')
        data_quality = query_metadata.get('data_quality', 1.0)
        
        # Log for debugging (but don't actually use adaptive logic)
        logger.info(f"Temperature selection: {final_temp:.2f} (base: 0.20, complexity: {complexity}, quality: {data_quality:.2f})")
        
        return final_temp
    
    @classmethod
    def calculate_query_complexity(cls, query: str, dataset: Dict) -> str:
        """
        Assess query complexity
        
        Returns: 'simple', 'medium', 'complex', 'very_complex'
        """
        
        # Factor 1: Query length
        word_count = len(query.split())
        
        # Factor 2: Multi-part query detection
        has_multiple_parts = any(connector in query.lower() for connector in [' and ', ' also ', ' plus ', 'additionally'])
        
        # Factor 3: Comparison/scenario complexity
        is_comparison = any(kw in query.lower() for kw in ['compare', 'vs', 'versus', 'difference'])
        is_scenario = 'what if' in query.lower()
        is_multi_factor = query.lower().count('and') >= 2
        
        # Factor 4: Dataset complexity
        dataset_size = len(dataset.get('properties', []))
        has_intelligence_layers = any(key in dataset for key in [
            'detected_trends', 'agent_profiles', 'cascade_prediction', 
            'liquidity_velocity', 'micromarkets'
        ])
        
        # Scoring
        complexity_score = 0
        
        # Query-based scoring
        if word_count < 5:
            complexity_score += 0
        elif word_count < 10:
            complexity_score += 1
        elif word_count < 20:
            complexity_score += 2
        else:
            complexity_score += 3
        
        if has_multiple_parts:
            complexity_score += 1
        
        if is_comparison:
            complexity_score += 2
        
        if is_scenario:
            complexity_score += 2
        
        if is_multi_factor:
            complexity_score += 1
        
        # Data-based scoring
        if dataset_size > 50:
            complexity_score += 1
        
        if has_intelligence_layers:
            complexity_score += 1
        
        # Classification
        if complexity_score <= 2:
            return 'simple'
        elif complexity_score <= 5:
            return 'medium'
        elif complexity_score <= 8:
            return 'complex'
        else:
            return 'very_complex'
    
    @classmethod
    def modulate_tone_for_confidence(cls, system_prompt: str, confidence_level: str, 
                                     data_quality: float) -> str:
        """
        Adjust system prompt to modulate tone based on confidence
        
        Args:
            system_prompt: Base system prompt
            confidence_level: 'high', 'medium', 'low'
            data_quality: 0.0-1.0 data quality score
        
        Returns: Enhanced system prompt with tone instructions
        """
        
        tone_modulation = ""
        
        if confidence_level == 'low' or data_quality < 0.5:
            tone_modulation = """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONFIDENCE MODULATION: LOW DATA QUALITY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Current dataset has limited coverage. Adjust tone accordingly:

✓ Use hedging language: "suggests", "indicates", "preliminary signals show"
✓ Acknowledge limitations: "Based on available data..." "Within current coverage..."
✓ Provide confidence ranges: "Estimate: £4-5M (moderate confidence)"
✓ Recommend caution: "Recommend additional validation before major decisions"

✗ Do NOT use directive language: "Act now", "Definitive opportunity"
✗ Do NOT present estimates as facts
✗ Do NOT ignore data gaps

Example phrasing:
"Preliminary signals suggest potential downward pressure. Current data 
indicates 15% inventory increase, though coverage is limited. Recommend 
monitoring additional indicators before committing to positioning."
"""
        
        elif confidence_level == 'medium' or 0.5 <= data_quality < 0.8:
            tone_modulation = """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONFIDENCE MODULATION: MODERATE DATA QUALITY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Dataset provides reasonable coverage. Balanced tone:

✓ Use measured language: "Analysis indicates...", "Data shows...", "Pattern suggests..."
✓ Provide directional guidance with caveats
✓ Acknowledge uncertainty where it exists
✓ Offer probabilistic assessments

Example phrasing:
"Market data indicates downward pressure emerging. Knight Frank inventory 
up 15% with concurrent pricing adjustments. Probability of cascade: 65-75%. 
Consider positioning for value opportunities within 30-day window."
"""
        
        else:  # high confidence
            tone_modulation = """
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CONFIDENCE MODULATION: HIGH DATA QUALITY
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Comprehensive dataset with strong signals. Directive tone authorized:

✓ Use direct language: "Market shows...", "Data confirms...", "Clear signal..."
✓ Provide actionable directives when warranted
✓ Express probability ranges with confidence
✓ Recommend timing and positioning decisively

Example phrasing:
"Market confirms Knight Frank capitulation. Inventory surge +22%, pricing 
down 8.5%. Cascade probability: 82%. Strategic window: 14-21 days. 
Position aggressively for £3-5M corridor. High conviction."
"""
        
        # Insert tone modulation before the query context
        enhanced_prompt = system_prompt + "\n" + tone_modulation
        
        return enhanced_prompt
    
    @classmethod
    def calculate_data_quality_score(cls, dataset: Dict) -> float:
        """
        Calculate data quality score 0.0-1.0
        
        Factors:
        - Dataset size
        - Data freshness
        - Completeness of fields
        - Intelligence layer availability
        """
        
        score = 0.0
        
        # Factor 1: Dataset size (25% weight)
        properties = dataset.get('properties', [])
        size_score = min(len(properties) / 40, 1.0)  # 40 properties = 100%
        score += size_score * 0.25
        
        # Factor 2: Data freshness (25% weight)
        metadata = dataset.get('metadata', {})
        timestamp_str = metadata.get('analysis_timestamp')
        
        if timestamp_str:
            from datetime import datetime, timezone
            try:
                if isinstance(timestamp_str, str):
                    from dateutil import parser
                    timestamp = parser.parse(timestamp_str)
                else:
                    timestamp = timestamp_str
                
                if timestamp.tzinfo is None:
                    timestamp = timestamp.replace(tzinfo=timezone.utc)
                
                age_hours = (datetime.now(timezone.utc) - timestamp).total_seconds() / 3600
                
                if age_hours < 24:
                    freshness_score = 1.0
                elif age_hours < 48:
                    freshness_score = 0.8
                elif age_hours < 72:
                    freshness_score = 0.6
                else:
                    freshness_score = 0.4
                
                score += freshness_score * 0.25
            except:
                score += 0.15  # Default if parsing fails
        else:
            score += 0.10
        
        # Factor 3: Field completeness (20% weight)
        if properties:
            # Check key fields
            sample_prop = properties[0]
            has_price = 'price' in sample_prop
            has_agent = 'agent' in sample_prop
            has_location = 'address' in sample_prop or 'submarket' in sample_prop
            has_sqft = 'price_per_sqft' in sample_prop or 'sqft' in sample_prop
            
            completeness = sum([has_price, has_agent, has_location, has_sqft]) / 4
            score += completeness * 0.20
        
        # Factor 4: Intelligence layers (30% weight)
        intelligence_layers = [
            'detected_trends',
            'agent_profiles',
            'cascade_prediction',
            'liquidity_velocity',
            'micromarkets'
        ]
        
        layers_present = sum(1 for layer in intelligence_layers if layer in dataset)
        intelligence_score = layers_present / len(intelligence_layers)
        score += intelligence_score * 0.30
        
        return round(score, 2)


def get_adaptive_llm_config(query: str, dataset: Dict, is_followup: bool = False, 
                           category: str = 'market_overview') -> Dict:
    """
    One-stop function to get all adaptive LLM parameters
    
    FIXED: Always returns temperature=0.2 and max_tokens=350
    
    Returns: {
        'temperature': float,
        'max_tokens': int,
        'complexity': str,
        'data_quality': float,
        'confidence_level': str
    }
    """
    
    controller = AdaptiveLLMController()
    
    # Calculate metrics
    complexity = controller.calculate_query_complexity(query, dataset)
    data_quality = controller.calculate_data_quality_score(dataset)
    
    # Determine confidence level
    if data_quality >= 0.75:
        confidence_level = 'high'
    elif data_quality >= 0.5:
        confidence_level = 'medium'
    else:
        confidence_level = 'low'
    
    # FIXED: Always institutional brevity
    temperature = 0.2
    max_tokens = 350
    
    logger.info(f"LLM Config: temp={temperature}, tokens={max_tokens}, complexity={complexity}, quality={data_quality:.2f}")
    
    return {
        'temperature': temperature,
        'max_tokens': max_tokens,
        'complexity': complexity,
        'data_quality': data_quality,
        'confidence_level': confidence_level,
        'query_type': 'analytical'
    }
