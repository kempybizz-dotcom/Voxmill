"""
VOXMILL AI ANALYZER - MULTI-REGION PRODUCTION VERSION
======================================================
✅ MULTI-REGION INTELLIGENCE: Analyzes combined + individual regions
✅ REGIONAL BREAKDOWN: Comparative analysis across submarkets
✅ BACKWARDS COMPATIBLE: Works with single or multiple regions
✅ GPT-4o powered with intelligent fallbacks

CRITICAL NEW FEATURES:
- Detects multi-region datasets automatically
- Generates region-by-region comparison
- Creates unified intelligence across all areas
- Maintains backwards compatibility with single region
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
    """Retry on 5xx errors (transient) but not 4xx (client errors)"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    error_str = str(e).lower()
                    
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


# ============================================================================
# 🔥 NEW: MULTI-REGION METRICS CALCULATOR
# ============================================================================

def calculate_regional_metrics(properties, regions):
    """
    Calculate metrics for EACH region individually + combined
    
    Returns: Dict with 'combined' metrics and per-region metrics
    """
    
    print(f"\n📊 CALCULATING REGIONAL METRICS")
    print(f"   Total Properties: {len(properties)}")
    print(f"   Regions: {', '.join(regions)}")
    
    # Combined metrics (all properties)
    combined_metrics = calculate_metrics_for_dataset(properties, "Combined")
    
    # Per-region metrics
    regional_metrics = {}
    
    for region in regions:
        # Filter properties for this region
        region_props = [p for p in properties if p.get('source_region') == region or p.get('area') == region]
        
        if len(region_props) > 0:
            regional_metrics[region] = calculate_metrics_for_dataset(region_props, region)
        else:
            print(f"   ⚠️  No properties found for region: {region}")
    
    return {
        'combined': combined_metrics,
        'by_region': regional_metrics
    }


def calculate_metrics_for_dataset(properties, label):
    """
    Calculate metrics for a specific dataset (combined or single region)
    """
    
    if not properties or len(properties) == 0:
        logger.error(f"CRITICAL: No properties for {label}")
        raise Exception(f"No properties to analyze for {label}")
    
    print(f"\n   📈 Metrics for {label}: {len(properties)} properties")
    
    prices = [p['price'] for p in properties if p.get('price', 0) > 0]
    
    # FIXED: Null-safe price_per_sqft extraction
    prices_per_sqft = [p['price_per_sqft'] for p in properties 
                       if p.get('price_per_sqft') is not None and p['price_per_sqft'] > 0]
    
    property_types = [p['property_type'] for p in properties if p.get('property_type')]
    
    if not prices:
        raise Exception(f"No valid price data for {label}")
    
    metrics = {
        'total_properties': len(properties),
        'avg_price': sum(prices) / len(prices),
        'median_price': sorted(prices)[len(prices) // 2],
        'min_price': min(prices),
        'max_price': max(prices),
        'avg_price_per_sqft': sum(prices_per_sqft) / len(prices_per_sqft) if prices_per_sqft else 0,
        'most_common_type': max(set(property_types), key=property_types.count) if property_types else 'Property'
    }
    
    print(f"      Avg Price: £{metrics['avg_price']:,.0f}")
    print(f"      Median: £{metrics['median_price']:,.0f}")
    print(f"      Price/SqFt: £{metrics['avg_price_per_sqft']:.0f}")
    
    return metrics


# ============================================================================
# 🔥 NEW: MULTI-REGION AI INTELLIGENCE GENERATOR
# ============================================================================

@retry_on_transient_error(max_retries=2, base_delay=2.0)
def generate_multi_region_intelligence(regional_metrics, regions, city):
    """
    Generate AI intelligence for MULTI-REGION analysis
    
    Uses GPT-4o to create comparative analysis across regions
    """
    
    print(f"\n🤖 GENERATING MULTI-REGION AI INTELLIGENCE")
    print(f"   Model: GPT-4o")
    print(f"   Regions: {', '.join(regions)}")
    print(f"   City: {city}")
    
    if not OPENAI_API_KEY:
        raise Exception("OPENAI_API_KEY not configured")
    
    try:
        # Build multi-region prompt
        combined = regional_metrics['combined']
        by_region = regional_metrics['by_region']
        
        # Create regional comparison data
        regional_comparison = ""
        for region, metrics in by_region.items():
            regional_comparison += f"\n{region}:\n"
            regional_comparison += f"- Properties: {metrics['total_properties']}\n"
            regional_comparison += f"- Avg Price: £{metrics['avg_price']:,.0f}\n"
            regional_comparison += f"- Price/SqFt: £{metrics['avg_price_per_sqft']:.0f}\n"
            regional_comparison += f"- Range: £{metrics['min_price']:,.0f} - £{metrics['max_price']:,.0f}\n"
        
        prompt = f"""You are an elite market intelligence analyst for luxury real estate in {city}.

MULTI-REGION ANALYSIS - {', '.join(regions)}

COMBINED MARKET DATA:
- Total Properties: {combined['total_properties']}
- Average Price: £{combined['avg_price']:,.0f}
- Median Price: £{combined['median_price']:,.0f}
- Price Range: £{combined['min_price']:,.0f} - £{combined['max_price']:,.0f}
- Avg Price/SqFt: £{combined['avg_price_per_sqft']:.0f}

REGIONAL BREAKDOWN:
{regional_comparison}

Generate a Fortune 500-level multi-region market intelligence report with:

1. EXECUTIVE SUMMARY (2-3 sentences)
   - BLUF (Bottom Line Up Front) covering all {len(regions)} regions
   - Comparative positioning across submarkets
   - Key cross-regional opportunities or arbitrage

2. STRATEGIC INSIGHTS (3-4 bullet points)
   - Regional price differentials and arbitrage opportunities
   - Competitive positioning by submarket
   - Cross-regional investment strategies
   - Market gaps across all regions

3. REGIONAL COMPARISON (1 paragraph)
   - Which region offers best value?
   - Which has highest velocity/demand?
   - Strategic plays unique to each submarket

4. TACTICAL OPPORTUNITIES (2-3 specific actions)
   - Immediate actions (this week) - which region to target first
   - Near-term plays (this month) - cross-regional strategy
   - Strategic positioning (this quarter) - portfolio allocation

5. RISK ASSESSMENT (2-3 sentences)
   - Regional concentration risks
   - Price volatility differences
   - Competitive threats by submarket

Format as JSON:
{{
  "executive_summary": "...",
  "strategic_insights": ["...", "...", "..."],
  "regional_comparison": "...",
  "tactical_opportunities": {{
    "immediate": "...",
    "near_term": "...",
    "strategic": "..."
  }},
  "risk_assessment": "...",
  "market_sentiment": "Bullish/Neutral/Bearish",
  "confidence_level": "High/Medium/Low"
}}

Use direct, confident language. No hedging. Board-room ready. Focus on cross-regional arbitrage."""

        print(f"   → Sending multi-region request to GPT-4o...")
        
        # Direct HTTP request to OpenAI API
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
                        'content': 'You are an elite luxury real estate market analyst specializing in multi-region comparative analysis. Your reports drive multi-million pound portfolio decisions. Be direct, confident, and actionable with clear regional recommendations.'
                    },
                    {
                        'role': 'user',
                        'content': prompt
                    }
                ],
                'temperature': 0.7,
                'max_tokens': 2000,
                'response_format': {'type': 'json_object'}
            },
            timeout=60
        )
        
        if response.status_code != 200:
            error_detail = response.text[:500] if response.text else 'Unknown error'
            raise Exception(f"OpenAI API error {response.status_code}: {error_detail}")
        
        api_response = response.json()
        
        if 'error' in api_response:
            raise Exception(f"OpenAI API error: {api_response['error'].get('message', 'Unknown error')}")
        
        if 'choices' not in api_response or len(api_response['choices']) == 0:
            raise Exception("No response from OpenAI API")
        
        content = api_response['choices'][0]['message']['content']
        intelligence = json.loads(content)
        
        print(f"   ✅ Multi-region AI intelligence generated")
        print(f"      Sentiment: {intelligence.get('market_sentiment', 'N/A')}")
        print(f"      Confidence: {intelligence.get('confidence_level', 'N/A')}")
        
        return intelligence
        
    except requests.exceptions.Timeout:
        print(f"   ⚠️  OpenAI API timeout - using fallback intelligence...")
        return generate_multi_region_fallback(regional_metrics, regions, city)
        
    except requests.exceptions.RequestException as e:
        print(f"   ⚠️  OpenAI API request failed: {str(e)}")
        print(f"   🔄 Using fallback intelligence...")
        return generate_multi_region_fallback(regional_metrics, regions, city)
        
    except json.JSONDecodeError as e:
        print(f"   ⚠️  Failed to parse OpenAI response: {str(e)}")
        print(f"   🔄 Using fallback intelligence...")
        return generate_multi_region_fallback(regional_metrics, regions, city)
        
    except Exception as e:
        print(f"   ⚠️  AI generation failed: {str(e)}")
        print(f"   🔄 Using fallback intelligence...")
        return generate_multi_region_fallback(regional_metrics, regions, city)


def generate_multi_region_fallback(regional_metrics, regions, city):
    """
    Generate fallback intelligence for multi-region analysis
    """
    
    print(f"   📊 Generating multi-region fallback intelligence...")
    
    combined = regional_metrics['combined']
    by_region = regional_metrics['by_region']
    
    # Find highest/lowest priced regions
    sorted_regions = sorted(
        by_region.items(),
        key=lambda x: x[1]['avg_price'],
        reverse=True
    )
    
    highest_region = sorted_regions[0][0] if sorted_regions else regions[0]
    lowest_region = sorted_regions[-1][0] if sorted_regions and len(sorted_regions) > 1 else regions[-1]
    
    highest_price = sorted_regions[0][1]['avg_price'] if sorted_regions else 0
    lowest_price = sorted_regions[-1][1]['avg_price'] if sorted_regions and len(sorted_regions) > 1 else 0
    
    price_differential = ((highest_price - lowest_price) / lowest_price * 100) if lowest_price > 0 else 0
    
    intelligence = {
        "executive_summary": f"Multi-region analysis of {combined['total_properties']} properties across {', '.join(regions)} reveals significant price differentials. {highest_region} commands premium positioning at £{highest_price:,.0f} average, while {lowest_region} offers value at £{lowest_price:,.0f} ({price_differential:.0f}% spread). Cross-regional arbitrage opportunities exist for strategic portfolio allocation.",
        
        "strategic_insights": [
            f"{highest_region} dominates premium tier with £{sorted_regions[0][1]['avg_price_per_sqft']:.0f}/sqft, positioning as market leader for ultra-high-net-worth targeting",
            f"{lowest_region} presents value entry at £{sorted_regions[-1][1]['avg_price_per_sqft']:.0f}/sqft, ideal for volume positioning and rapid portfolio scaling",
            f"Price differential of {price_differential:.0f}% between {highest_region} and {lowest_region} creates clear arbitrage window for cross-regional investment strategies",
            f"Combined market depth of {combined['total_properties']} properties across {len(regions)} submarkets enables diversified portfolio construction with controlled concentration risk"
        ],
        
        "regional_comparison": f"{highest_region} leads in absolute pricing but may face demand ceiling, while {lowest_region} offers liquidity advantage at lower entry points. Mid-tier regions {'and '.join(r[0] for r in sorted_regions[1:-1])} if len(sorted_regions) > 2 else '' provide balanced risk/return profiles for institutional positioning.",
        
        "tactical_opportunities": {
            "immediate": f"Target {lowest_region} for rapid accumulation at £{lowest_price:,.0f} average - value positioning creates immediate acquisition window before market repricing",
            
            "near_term": f"Build cross-regional portfolio: 40% {highest_region} (premium capture), 35% {lowest_region} (volume/velocity), 25% balanced across remaining submarkets for diversification",
            
            "strategic": f"Establish dominant presence across all {len(regions)} regions with focus on properties in £{int(combined['median_price'] * 0.9):,.0f}-£{int(combined['median_price'] * 1.1):,.0f} corridor for optimal liquidity and conversion velocity"
        },
        
        "risk_assessment": f"Regional concentration in {highest_region} creates price compression risk if premium segment softens. {lowest_region} faces potential demand ceiling as value buyers may have limited capacity. Monitor cross-regional capital flows weekly for early signals of preference shifts that could impact relative valuations and transaction velocity.",
        
        "market_sentiment": "Neutral" if price_differential < 30 else "Bullish",
        "confidence_level": "High" if len(by_region) >= len(regions) else "Medium",
        "data_source": "data_driven_fallback",
        "ai_disclaimer": "Analysis generated from statistical models (OpenAI API unavailable)"
    }
    
    print(f"   ✅ Multi-region fallback intelligence generated")
    
    return intelligence


# ============================================================================
# SINGLE-REGION AI INTELLIGENCE (BACKWARDS COMPATIBLE)
# ============================================================================

@retry_on_transient_error(max_retries=2, base_delay=2.0)
def generate_ai_intelligence(metrics, area, city):
    """
    Generate AI-powered market intelligence using GPT-4o (SINGLE REGION)
    BACKWARDS COMPATIBLE with original function signature
    """
    
    print(f"\n🤖 GENERATING AI INTELLIGENCE")
    print(f"   Model: GPT-4o")
    print(f"   Market: {area}, {city}")
    
    if not OPENAI_API_KEY:
        raise Exception("OPENAI_API_KEY not configured")
    
    try:
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
        
        if response.status_code != 200:
            error_detail = response.text[:500] if response.text else 'Unknown error'
            raise Exception(f"OpenAI API error {response.status_code}: {error_detail}")
        
        api_response = response.json()
        
        if 'error' in api_response:
            raise Exception(f"OpenAI API error: {api_response['error'].get('message', 'Unknown error')}")
        
        if 'choices' not in api_response or len(api_response['choices']) == 0:
            raise Exception("No response from OpenAI API")
        
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
    Generate fallback intelligence when OpenAI API is unavailable (SINGLE REGION)
    BACKWARDS COMPATIBLE
    """
    
    print(f"   📊 Generating data-driven fallback intelligence...")
    
    price_volatility = (metrics['max_price'] - metrics['min_price']) / metrics['avg_price']
    market_tier = "ultra-premium" if metrics['avg_price'] > 5000000 else "premium" if metrics['avg_price'] > 2000000 else "luxury"
    
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
            f"Market demonstrates {'high' if price_volatility > 0.5 else 'moderate'} price volatility ({int(price_volatility * 100)}%) indicating {'dynamic' if price_volatility > 0.5 else 'stable'} trading conditions"
        ],
        
        "tactical_opportunities": {
            "immediate": f"Target properties in £{int(metrics['median_price'] * 0.9):,.0f}-£{int(metrics['median_price'] * 1.1):,.0f} corridor for rapid conversion. Current median of £{metrics['median_price']:,.0f} represents sweet spot for buyer activity.",
            
            "near_term": f"Position {metrics['most_common_type'].lower()} inventory strategically against market saturation. Consider counter-positioning with alternative property types for competitive advantage.",
            
            "strategic": f"Build long-term presence in {area}'s {market_tier} segment with focus on properties above £{int(metrics['avg_price'] * 1.2):,.0f}. Market fundamentals support sustained activity in top-tier positioning."
        },
        
        "risk_assessment": f"Market concentration in {metrics['most_common_type'].lower()} properties creates potential supply pressure. Price volatility of {int(price_volatility * 100)}% requires careful timing. Monitor £/sqft trends weekly for early signals of directional shifts.",
        
        "market_sentiment": sentiment,
        "confidence_level": confidence,
        "data_source": "data_driven_fallback",
        "ai_disclaimer": "Analysis generated from statistical models (OpenAI API unavailable)"
    }
    
    print(f"   ✅ Fallback intelligence generated")
    
    return intelligence


# ============================================================================
# DEAL SCORING (UNCHANGED - NULL-SAFE)
# ============================================================================

def calculate_deal_scores(properties):
    """
    Calculate deal scores (1-10) for properties
    FIXED: Null-safe price_per_sqft filtering
    """
    
    print(f"\n📊 CALCULATING DEAL SCORES")
    
    if not properties or len(properties) == 0:
        return []
    
    prices_per_sqft = [p['price_per_sqft'] for p in properties 
                       if p.get('price_per_sqft') is not None and p['price_per_sqft'] > 0]
    
    if not prices_per_sqft:
        avg_price = sum(p['price'] for p in properties) / len(properties)
        
        scored = []
        for prop in properties:
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
        
        if ppf is None or ppf == 0:
            score = 5
            reason = "No price/sqft data"
        else:
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
    
    scored.sort(key=lambda x: x['deal_score'], reverse=True)
    
    return scored


def calculate_metrics(properties):
    """
    Calculate market metrics from property data (BACKWARDS COMPATIBLE)
    """
    return calculate_metrics_for_dataset(properties, "Market")


# ============================================================================
# MAIN ANALYSIS ORCHESTRATOR (UPDATED FOR MULTI-REGION)
# ============================================================================

def analyze_market_data():
    """
    Main analysis orchestrator
    ✅ AUTOMATICALLY DETECTS: Single vs Multi-region datasets
    """
    
    print("\n" + "="*70)
    print("VOXMILL AI ANALYSIS ENGINE (MULTI-REGION)")
    print("="*70)
    
    try:
        # Load data
        with open(INPUT_FILE, 'r') as f:
            data = json.load(f)
        
        metadata = data['metadata']
        properties = data['raw_data'].get('properties', [])
        
        # ✅ DETECT MULTI-REGION
        regions = metadata.get('regions', [metadata.get('area')])
        is_multi_region = len(regions) > 1
        
        print(f"Mode: {'MULTI-REGION' if is_multi_region else 'SINGLE REGION'}")
        print(f"Regions: {', '.join(regions)}")
        print(f"City: {metadata['city']}")
        print(f"Properties: {len(properties)}")
        print("="*70)
        
        if is_multi_region:
            # ✅ MULTI-REGION FLOW
            regional_metrics = calculate_regional_metrics(properties, regions)
            
            # Generate multi-region intelligence
            intelligence = generate_multi_region_intelligence(
                regional_metrics,
                regions,
                metadata['city']
            )
            
            # Use combined metrics for main analysis
            metrics = regional_metrics['combined']
            
            # Add regional breakdown to intelligence
            intelligence['regional_metrics'] = regional_metrics['by_region']
            
        else:
            # ✅ SINGLE-REGION FLOW (BACKWARDS COMPATIBLE)
            metrics = calculate_metrics(properties)
            
            intelligence = generate_ai_intelligence(
                metrics,
                metadata['area'],
                metadata['city']
            )
        
        # Calculate deal scores (same for both modes)
        scored_properties = calculate_deal_scores(properties)
        
        # Build analysis output
        analysis = {
            'metadata': {
                **metadata,
                'analysis_timestamp': datetime.now().isoformat(),
                'ai_model': 'gpt-4o-direct-http',
                'is_multi_region': is_multi_region
            },
            'metrics': metrics,
            'intelligence': intelligence,
            'properties': scored_properties[:15],
            'top_opportunities': scored_properties[:8]
        }
        
        # Save analysis
        with open(OUTPUT_FILE, 'w') as f:
            json.dump(analysis, f, indent=2)
        
        print(f"\n" + "="*70)
        print("✅ AI ANALYSIS COMPLETE")
        print("="*70)
        print(f"Output: {OUTPUT_FILE}")
        print(f"Mode: {'Multi-Region' if is_multi_region else 'Single Region'}")
        print(f"Properties Analyzed: {len(properties)}")
        if is_multi_region:
            print(f"Regions: {', '.join(regions)}")
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
