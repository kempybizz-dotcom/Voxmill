"""
VOXMILL AI ANALYZER - PRODUCTION VERSION
=========================================
GPT-4o powered market intelligence generation
Uses direct HTTP requests - bypasses OpenAI library proxy issues
FIXED: Null-safe price_per_sqft filtering throughout
"""

import os
import json
import requests
import logging
from datetime import datetime
import time
from functools import wraps

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def retry_on_transient_error(max_retries=2, base_delay=1.0):
    """
    Retry on 5xx errors (transient) but not 4xx (client errors)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    error_str = str(e).lower()
                    
                    # Check if it's a transient error (5xx)
                    is_transient = any(code in error_str for code in ['500', '502', '503', '504', 'timeout'])
                    
                    if is_transient and attempt < max_retries - 1:
                        delay = base_delay * (2 ** attempt)
                        print(f"   ⚠️  OpenAI transient error, retrying in {delay}s...")
                        time.sleep(delay)
                    else:
                        raise
            return None
        return wrapper
    return decorator

# Environment
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
WORKSPACE = os.environ.get('VOXMILL_WORKSPACE', '/tmp')
INPUT_FILE = os.path.join(WORKSPACE, "voxmill_raw_data.json")
OUTPUT_FILE = os.path.join(WORKSPACE, "voxmill_analysis.json")

@retry_on_transient_error(max_retries=2, base_delay=2.0)
def generate_ai_intelligence(metrics, area, city):
    """
    Generate AI-powered market intelligence using GPT-4o.
    Uses direct HTTP requests to bypass Render proxy issues.
    Includes retry logic for transient 5xx errors.
    """
    
    print(f"\n🤖 GENERATING AI INTELLIGENCE")
    print(f"   Model: GPT-4o")
    print(f"   Market: {area}, {city}")
    
    if not OPENAI_API_KEY:
        raise Exception("OPENAI_API_KEY not configured")
    
    try:
        # Build comprehensive prompt
        prompt = f"""You are an elite market intelligence analyst for luxury real estate in {area}, {city}.

MARKET DATA:
- Total Properties Analyzed: {metrics['total_properties']}
- Average Price: £{metrics['avg_price']:,.0f}
- Median Price: £{metrics['median_price']:,.0f}
- Price Range: £{metrics['min_price']:,.0f} - £{metrics['max_price']:,.0f}
- Average Price/SqFt: £{metrics['avg_price_per_sqft']:.0f}
- Most Common Type: {metrics['most_common_type']}

Generate a Fortune 500-level market intelligence report with:

1. EXECUTIVE SUMMARY (2-3 sentences)
   - BLUF (Bottom Line Up Front) - immediate actionable insight
   - Market positioning statement
   - Key opportunity or risk

2. STRATEGIC INSIGHTS (3-4 bullet points)
   - Pricing trends and anomalies
   - Competitive positioning opportunities
   - Market gaps or oversupply signals
   - Timing recommendations (buy/sell/hold)

3. TACTICAL OPPORTUNITIES (2-3 specific actions)
   - Immediate actions (this week)
   - Near-term plays (this month)
   - Strategic positioning (this quarter)

4. RISK ASSESSMENT (2-3 sentences)
   - Market headwinds
   - Overpricing risks
   - Competitive threats

Format as JSON:
{{
  "executive_summary": "...",
  "strategic_insights": ["...", "...", "..."],
  "tactical_opportunities": {{
    "immediate": "...",
    "near_term": "...",
    "strategic": "..."
  }},
  "risk_assessment": "...",
  "market_sentiment": "Bullish/Neutral/Bearish",
  "confidence_level": "High/Medium/Low"
}}

Use direct, confident language. No hedging. Board-room ready."""

        print(f"   → Sending request to GPT-4o via direct HTTP...")
        
        # Direct HTTP request to OpenAI API - bypasses library proxy issues
        response = requests.post(
            'https://api.openai.com/v1/chat/completions',
            headers={
                'Authorization': f'Bearer {OPENAI_API_KEY}',
                'Content-Type': 'application/json'
            },
            json={
                'model': 'gpt-4o',
                'messages': [
                    {
                        'role': 'system',
                        'content': 'You are an elite luxury real estate market analyst. Your reports drive multi-million pound decisions. Be direct, confident, and actionable.'
                    },
                    {
                        'role': 'user',
                        'content': prompt
                    }
                ],
                'temperature': 0.7,
                'max_tokens': 1500,
                'response_format': {'type': 'json_object'}
            },
            timeout=60
        )
        
        # Check response
        if response.status_code != 200:
            error_detail = response.text[:500] if response.text else 'Unknown error'
            raise Exception(f"OpenAI API error {response.status_code}: {error_detail}")
        
        # Parse response
        api_response = response.json()
        
        if 'error' in api_response:
            raise Exception(f"OpenAI API error: {api_response['error'].get('message', 'Unknown error')}")
        
        if 'choices' not in api_response or len(api_response['choices']) == 0:
            raise Exception("No response from OpenAI API")
        
        # Extract intelligence
        content = api_response['choices'][0]['message']['content']
        intelligence = json.loads(content)
        
        print(f"   ✅ AI intelligence generated via direct HTTP")
        print(f"      Sentiment: {intelligence.get('market_sentiment', 'N/A')}")
        print(f"      Confidence: {intelligence.get('confidence_level', 'N/A')}")
        
        return intelligence
        
    except requests.exceptions.Timeout:
        print(f"   ⚠️  OpenAI API timeout - using fallback intelligence...")
        return generate_fallback_intelligence(metrics, area, city)
        
    except requests.exceptions.RequestException as e:
        print(f"   ⚠️  OpenAI API request failed: {str(e)}")
        print(f"   🔄 Using fallback intelligence...")
        return generate_fallback_intelligence(metrics, area, city)
        
    except json.JSONDecodeError as e:
        print(f"   ⚠️  Failed to parse OpenAI response: {str(e)}")
        print(f"   🔄 Using fallback intelligence...")
        return generate_fallback_intelligence(metrics, area, city)
        
    except Exception as e:
        print(f"   ⚠️  AI generation failed: {str(e)}")
        print(f"   🔄 Using fallback intelligence...")
        return generate_fallback_intelligence(metrics, area, city)


def generate_fallback_intelligence(metrics, area, city):
    """
    Generate fallback intelligence when OpenAI API is unavailable.
    Still produces professional, data-driven insights.
    """
    
    print(f"   📊 Generating data-driven fallback intelligence...")
    
    # Calculate additional insights
    price_volatility = (metrics['max_price'] - metrics['min_price']) / metrics['avg_price']
    market_tier = "ultra-premium" if metrics['avg_price'] > 5000000 else "premium" if metrics['avg_price'] > 2000000 else "luxury"
    
    # Determine sentiment based on metrics
    if metrics['avg_price_per_sqft'] > 1500:
        sentiment = "Bullish"
        confidence = "High"
    elif metrics['avg_price_per_sqft'] > 1000:
        sentiment = "Neutral"
        confidence = "Medium"
    else:
        sentiment = "Bearish"
        confidence = "Medium"
    
    intelligence = {
        "executive_summary": f"Market analysis of {metrics['total_properties']} {market_tier} properties in {area} reveals average pricing at £{metrics['avg_price']:,.0f} with {int(price_volatility * 100)}% price volatility. Current market conditions suggest {sentiment.lower()} positioning with median transactions at £{metrics['median_price']:,.0f}. Strategic opportunities exist in the £{int(metrics['median_price'] * 0.85):,.0f}-£{int(metrics['median_price'] * 1.15):,.0f} range for optimal conversion.",
        
        "strategic_insights": [
            f"Price distribution from £{metrics['min_price']:,.0f} to £{metrics['max_price']:,.0f} indicates diverse market segmentation with opportunities across multiple tiers",
            f"{metrics['most_common_type']} properties represent dominant inventory type - positioning against this segment critical for differentiation",
            f"Average £{metrics['avg_price_per_sqft']:.0f}/sqft suggests {'premium' if metrics['avg_price_per_sqft'] > 1200 else 'competitive'} pricing environment with {'limited' if metrics['avg_price_per_sqft'] > 1200 else 'strong'} value positioning opportunities",
            f"Market demonstrates {'high' if price_volatility > 0.5 else 'moderate'} price volatility ({int(price_volatility * 100)}%) indicating {'dynamic' if price_volatility > 0.5 else 'stable'} trading conditions and {'aggressive' if price_volatility > 0.5 else 'measured'} pricing strategies"
        ],
        
        "tactical_opportunities": {
            "immediate": f"Target properties in £{int(metrics['median_price'] * 0.9):,.0f}-£{int(metrics['median_price'] * 1.1):,.0f} corridor for rapid conversion. Current median of £{metrics['median_price']:,.0f} represents sweet spot for buyer activity and inventory turnover.",
            
            "near_term": f"Position {metrics['most_common_type'].lower()} inventory strategically against market saturation. Consider counter-positioning with alternative property types for competitive advantage and premium capture.",
            
            "strategic": f"Build long-term presence in {area}'s {market_tier} segment with focus on properties above £{int(metrics['avg_price'] * 1.2):,.0f}. Market fundamentals support sustained activity in top-tier positioning through strategic acquisition and selective listing management."
        },
        
        "risk_assessment": f"Market concentration in {metrics['most_common_type'].lower()} properties (dominant type) creates potential supply pressure and pricing compression risk. Price volatility of {int(price_volatility * 100)}% suggests active trading but requires careful timing and positioning. Monitor £/sqft trends weekly for early signals of directional shifts or competitive repositioning that could impact valuations and transaction velocity.",
        
        "market_sentiment": sentiment,
        "confidence_level": confidence,
        "data_source": "data_driven_fallback",
        "ai_disclaimer": "Analysis generated from statistical models (OpenAI API unavailable)"
    }
    
    print(f"   ✅ Fallback intelligence generated")
    print(f"      Sentiment: {sentiment}")
    print(f"      Confidence: {confidence}")
    
    return intelligence


def calculate_deal_scores(properties):
    """
    Calculate deal scores (1-10) for properties.
    FIXED: Null-safe price_per_sqft filtering
    """
    
    print(f"\n📊 CALCULATING DEAL SCORES")
    
    if not properties or len(properties) == 0:
        return []
    
    # FIXED: Null-safe price/sqft extraction
    prices_per_sqft = [p['price_per_sqft'] for p in properties 
                       if p.get('price_per_sqft') is not None and p['price_per_sqft'] > 0]
    
    if not prices_per_sqft:
        # No price/sqft data - use price only
        avg_price = sum(p['price'] for p in properties) / len(properties)
        
        scored = []
        for prop in properties:
            # Lower price = higher score
            price_ratio = prop['price'] / avg_price if avg_price > 0 else 1
            score = max(1, min(10, int(11 - (price_ratio * 5))))
            
            scored.append({
                **prop,
                'deal_score': score,
                'score_reason': f"Price: £{prop['price']:,.0f}"
            })
        
        return scored
    
    avg_price_per_sqft = sum(prices_per_sqft) / len(prices_per_sqft)
    
    scored = []
    for prop in properties:
        ppf = prop.get('price_per_sqft')
        
        # FIXED: Explicit None check
        if ppf is None or ppf == 0:
            score = 5
            reason = "No price/sqft data"
        else:
            # Lower price/sqft = higher score
            ratio = ppf / avg_price_per_sqft
            
            if ratio < 0.7:
                score = 9
                reason = f"Underpriced (£{ppf:.0f}/sqft vs £{avg_price_per_sqft:.0f} avg)"
            elif ratio < 0.85:
                score = 8
                reason = f"Good value (£{ppf:.0f}/sqft)"
            elif ratio < 1.0:
                score = 7
                reason = f"Fair price (£{ppf:.0f}/sqft)"
            elif ratio < 1.15:
                score = 6
                reason = f"Slightly high (£{ppf:.0f}/sqft)"
            elif ratio < 1.3:
                score = 5
                reason = f"Overpriced (£{ppf:.0f}/sqft)"
            else:
                score = 4
                reason = f"Significantly overpriced (£{ppf:.0f}/sqft)"
        
        scored.append({
            **prop,
            'deal_score': score,
            'score_reason': reason
        })
    
    print(f"   ✅ Scored {len(scored)} properties")
    
    # Sort by score (highest first)
    scored.sort(key=lambda x: x['deal_score'], reverse=True)
    
    return scored


def calculate_metrics(properties):
    """
    Calculate market metrics from property data.
    FIXED: Null-safe price_per_sqft filtering
    """
    
    print(f"\n📈 CALCULATING MARKET METRICS")
    
    # STRICT VALIDATION
    if not properties or len(properties) == 0:
        logger.error("CRITICAL: No properties returned by data collector")
        raise Exception("No properties to analyze - data collection failed")
    
    if len(properties) < 5:
        logger.warning(f"Only {len(properties)} properties found - results may be unreliable")
        # Don't raise, but warn user
    
    prices = [p['price'] for p in properties if p.get('price', 0) > 0]
    
    # FIXED: Null-safe price_per_sqft extraction
    prices_per_sqft = [p['price_per_sqft'] for p in properties 
                       if p.get('price_per_sqft') is not None and p['price_per_sqft'] > 0]
    
    property_types = [p['property_type'] for p in properties if p.get('property_type')]
    
    if not prices:
        raise Exception("No valid price data")
    
    # Calculate metrics
    metrics = {
        'total_properties': len(properties),
        'avg_price': sum(prices) / len(prices),
        'median_price': sorted(prices)[len(prices) // 2],
        'min_price': min(prices),
        'max_price': max(prices),
        'avg_price_per_sqft': sum(prices_per_sqft) / len(prices_per_sqft) if prices_per_sqft else 0,
        'most_common_type': max(set(property_types), key=property_types.count) if property_types else 'Property'
    }
    
    print(f"   ✅ Metrics calculated")
    print(f"      Properties: {metrics['total_properties']}")
    print(f"      Avg Price: £{metrics['avg_price']:,.0f}")
    print(f"      Median: £{metrics['median_price']:,.0f}")
    
    return metrics


def analyze_market_data():
    """Main analysis orchestrator."""
    
    print("\n" + "="*70)
    print("VOXMILL AI ANALYSIS ENGINE")
    print("="*70)
    
    try:
        # Load data
        with open(INPUT_FILE, 'r') as f:
            data = json.load(f)
        
        metadata = data['metadata']
        properties = data['raw_data'].get('properties', [])
        
        print(f"Vertical: {metadata['vertical']}")
        print(f"Area: {metadata['area']}")
        print(f"City: {metadata['city']}")
        print(f"Properties: {len(properties)}")
        print("="*70)
        
        # Calculate metrics
        metrics = calculate_metrics(properties)
        
        # Generate AI intelligence (tries GPT-4o, falls back if needed)
        intelligence = generate_ai_intelligence(
            metrics,
            metadata['area'],
            metadata['city']
        )
        
        # Calculate deal scores
        scored_properties = calculate_deal_scores(properties)
        
        # Build analysis output
        analysis = {
            'metadata': {
                **metadata,
                'analysis_timestamp': datetime.now().isoformat(),
                'ai_model': 'gpt-4o-direct-http'
            },
            'metrics': metrics,
            'intelligence': intelligence,
            'properties': scored_properties[:15],  # Top 15 for PDF
            'top_opportunities': scored_properties[:8]  # Top 8 for highlights
        }
        
        # Save analysis
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(analysis, f, indent=2)
        
        print(f"\n" + "="*70)
        print("✅ AI ANALYSIS COMPLETE")
        print("="*70)
        print(f"Output: {OUTPUT_FILE}")
        print(f"Properties Analyzed: {len(properties)}")
        print(f"Top Opportunities: {len(analysis['top_opportunities'])}")
        print(f"Sentiment: {intelligence.get('market_sentiment', 'N/A')}")
        print("="*70)
        
        return OUTPUT_FILE
        
    except Exception as e:
        print(f"\n❌ ANALYSIS FAILED")
        print(f"   Error: {str(e)}")
        raise


if __name__ == "__main__":
    try:
        analyze_market_data()
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        exit(1)
