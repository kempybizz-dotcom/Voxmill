"""
VOXMILL ADAPTIVE LLM CONTROLLER
================================
Dynamic temperature selection and confidence-based tone modulation

Makes the LLM genuinely intelligent:
- Simple queries → Low temp (0.3) for consistency
- Complex analysis → Higher temp (0.5-0.7) for creativity
- Low data quality → Hedged language
- High confidence → Directive language
"""

import logging
from typing import Dict, Tuple

logger = logging.getLogger(__name__)


class AdaptiveLLMController:
    """Controls LLM parameters based on query and data context"""
    
    # Temperature ranges by query type
    TEMPERATURE_MAP = {
        'factual': 0.3,           # "What's the average price?"
        'analytical': 0.5,        # "Analyze competitive landscape"
        'predictive': 0.6,        # "What if Knight Frank drops 10%?"
        'strategic': 0.7,         # "Develop strategic positioning"
        'creative': 0.8,          # "Generate alternative scenarios"
        'conversational': 0.4     # Greetings, follow-ups
    }
    
    @classmethod
    def determine_optimal_temperature(cls, query: str, query_metadata: Dict) -> float:
        """
        Determine optimal temperature based on query characteristics
        
        Args:
            query: User query text
            query_metadata: {
                'category': str,
                'complexity': str,
                'is_followup': bool,
                'data_quality': float
            }
        
        Returns: Temperature value 0.0-1.0
        """
        
        query_lower = query.lower()
        category = query_metadata.get('category', 'market_overview')
        complexity = query_metadata.get('complexity', 'medium')
        
        # RULE 1: Determine base temperature from query type
        base_temp = cls._classify_query_type(query_lower, category)
        
        # RULE 2: Adjust for complexity
        complexity_adjustment = {
            'simple': -0.1,
            'medium': 0.0,
            'complex': +0.1,
            'very_complex': +0.2
        }
        
        adjusted_temp = base_temp + complexity_adjustment.get(complexity, 0.0)
        
        # RULE 3: Adjust for data quality
        data_quality = query_metadata.get('data_quality', 1.0)
        
        if data_quality < 0.5:
            # Low data quality → reduce temperature (more conservative)
            adjusted_temp *= 0.9
        
        # RULE 4: Adjust for follow-up queries
        if query_metadata.get('is_followup'):
            # Follow-ups should be slightly more conversational
            adjusted_temp += 0.05
        
        # Clamp to valid range
        final_temp = max(0.3, min(adjusted_temp, 0.8))
        
        logger.info(f"Temperature selection: {final_temp:.2f} (base: {base_temp:.2f}, complexity: {complexity}, quality: {data_quality:.2f})")
        
        return round(final_temp, 2)
    
    @classmethod
    def _classify_query_type(cls, query: str, category: str) -> float:
        """Classify query to determine base temperature"""
        
        # Factual queries (low temperature)
        factual_keywords = ['what is', 'how many', 'price', 'average', 'total', 'count']
        if any(kw in query for kw in factual_keywords):
            return cls.TEMPERATURE_MAP['factual']
        
        # Predictive queries (higher temperature)
        predictive_keywords = ['what if', 'predict', 'forecast', 'scenario', 'will']
        if any(kw in query for kw in predictive_keywords):
            return cls.TEMPERATURE_MAP['predictive']
        
        # Strategic queries (high temperature)
        strategic_keywords = ['strategy', 'recommend', 'should i', 'best approach', 'positioning']
        if any(kw in query for kw in strategic_keywords):
            return cls.TEMPERATURE_MAP['strategic']
        
        # Analytical queries (medium temperature)
        analytical_keywords = ['analyze', 'compare', 'evaluate', 'assess', 'competitive']
        if any(kw in query for kw in analytical_keywords):
            return cls.TEMPERATURE_MAP['analytical']
        
        # Conversational queries
        conversational_keywords = ['hello', 'hi', 'thanks', 'thank you', 'great']
        if any(kw in query for kw in conversational_keywords):
            return cls.TEMPERATURE_MAP['conversational']
        
        # Default: use category-based temperature
        category_temps = {
            'market_overview': 0.4,
            'competitive_landscape': 0.5,
            'scenario_modelling': 0.6,
            'strategic_outlook': 0.7,
            'opportunities': 0.5
        }
        
        return category_temps.get(category, 0.5)
    
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
    
    @classmethod
    def get_optimal_max_tokens(cls, complexity: str, query_type: str) -> int:
        """Determine optimal max_tokens based on query needs"""
        
        # Base token limits by query type
        base_limits = {
            'factual': 500,           # Brief, concise answers
            'analytical': 1200,       # Moderate depth
            'predictive': 1500,       # Detailed scenarios
            'strategic': 2000,        # Comprehensive analysis
            'conversational': 300     # Quick responses
        }
        
        base_limit = base_limits.get(query_type, 1200)
        
        # Adjust for complexity
        complexity_multipliers = {
            'simple': 0.7,
            'medium': 1.0,
            'complex': 1.3,
            'very_complex': 1.5
        }
        
        multiplier = complexity_multipliers.get(complexity, 1.0)
        
        optimal_tokens = int(base_limit * multiplier)
        
        # Clamp to reasonable range
        return max(300, min(optimal_tokens, 2500))


def get_adaptive_llm_config(query: str, dataset: Dict, is_followup: bool = False, 
                           category: str = 'market_overview') -> Dict:
    """
    One-stop function to get all adaptive LLM parameters
    
    Returns: {
        'temperature': float,
        'max_tokens': int,
        'complexity': str,
        'data_quality': float,
        'confidence_level': str,
        'tone_modulation': str
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
    
    # Build query metadata
    query_metadata = {
        'category': category,
        'complexity': complexity,
        'is_followup': is_followup,
        'data_quality': data_quality
    }
    
    # Get optimal parameters
    temperature = controller.determine_optimal_temperature(query, query_metadata)
    
    # Classify query type for max_tokens
    query_type = 'analytical'  # Default
    if 'what if' in query.lower() or 'scenario' in query.lower():
        query_type = 'predictive'
    elif any(kw in query.lower() for kw in ['strategy', 'recommend', 'should']):
        query_type = 'strategic'
    elif any(kw in query.lower() for kw in ['what is', 'price', 'count']):
        query_type = 'factual'
    
    max_tokens = controller.get_optimal_max_tokens(complexity, query_type)
    
    return {
        'temperature': temperature,
        'max_tokens': max_tokens,
        'complexity': complexity,
        'data_quality': data_quality,
        'confidence_level': confidence_level,
        'query_type': query_type
    }
