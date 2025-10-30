"""
VOXMILL AI ANALYZER - PRODUCTION VERSION
=========================================
GPT-4o powered market intelligence generation
Fixed for Render proxy environment
"""

import os
import json
from datetime import datetime
from openai import OpenAI

# Environment
OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
INPUT_FILE = "/tmp/voxmill_raw_data.json"
OUTPUT_FILE = "/tmp/voxmill_analysis.json"

def generate_ai_intelligence(metrics, area, city):
    """
    Generate AI-powered market intelligence using GPT-4o.
    Fixed for Render environment compatibility.
    """
    
    print(f"\n🤖 GENERATING AI INTELLIGENCE")
    print(f"   Model: GPT-4o")
    print(f"   Market: {area}, {city}")
    
    if not OPENAI_API_KEY:
        raise Exception("OPENAI_API_KEY not configured")
    
    try:
        # Initialize OpenAI client - FIXED for Render proxy
        # Don't pass any proxy-related arguments - let the library handle it
        client = OpenAI(
            api_key=OPENAI_API_KEY,
            timeout=60.0,
            max_retries=2
        )
        
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

        print(f"   → Sending request to GPT-4o...")
        
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {
                    "role": "system",
                    "content": "You are an elite luxury real estate market analyst. Your reports drive multi-million pound decisions. Be direct, confident, and actionable."
                },
                {
                    "role": "user",
                    "content": prompt
                }
            ],
            temperature=0.7,
            max_tokens=1500,
            response_format={"type": "json_object"}
        )
        
        intelligence = json.loads(response.choices[0].message.content)
        
        print(f"   ✅ AI intelligence generated")
        print(f"      Sentiment: {intelligence.get('market_sentiment', 'N/A')}")
        print(f"      Confidence: {intelligence.get('confidence_level', 'N/A')}")
        
        return intelligence
        
    except Exception as e:
        print(f"   ❌ AI generation failed: {str(e)}")
        
        # Fallback intelligence if API fails
        print(f"   🔄 Using fallback intelligence...")
        
        return {
            "executive_summary": f"Market analysis of {metrics['total_properties']} luxury properties in {area}. Average price point of £{metrics['avg_price']:,.0f} indicates premium positioning. Strategic opportunities exist in the £{metrics['median_price']:,.0f}-£{int(metrics['median_price'] * 1.2):,.0f} range.",
            "strategic_insights": [
                f"Price range of £{metrics['min_price']:,.0f}-£{metrics['max_price']:,.0f} shows market diversity",
                f"{metrics['most_common_type']} properties dominate current listings",
                f"Average £{metrics['avg_price_per_sqft']:.0f}/sqft suggests competitive positioning opportunity",
                "Market shows activity across multiple property types and price points"
            ],
            "tactical_opportunities": {
                "immediate": f"Focus on properties in the £{int(metrics['median_price'] * 0.9):,.0f}-£{int(metrics['median_price'] * 1.1):,.0f} range for quick conversion",
                "near_term": f"Position {metrics['most_common_type'].lower()} inventory for seasonal demand",
                "strategic": f"Build presence in {area}'s premium segment (£{int(metrics['avg_price'] * 1.2):,.0f}+)"
            },
            "risk_assessment": f"Market concentration in {metrics['most_common_type'].lower()} properties may indicate supply pressure. Monitor pricing trends weekly for early signals of shifts.",
            "market_sentiment": "Neutral",
            "confidence_level": "Medium"
        }


def calculate_deal_scores(properties):
    """Calculate deal scores (1-10) for properties."""
    
    print(f"\n📊 CALCULATING DEAL SCORES")
    
    if not properties or len(properties) == 0:
        return []
    
    # Get price/sqft for all properties
    prices_per_sqft = [p['price_per_sqft'] for p in properties if p.get('price_per_sqft', 0) > 0]
    
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
        ppf = prop.get('price_per_sqft', 0)
        
        if ppf == 0:
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
    """Calculate market metrics from property data."""
    
    print(f"\n📈 CALCULATING MARKET METRICS")
    
    if not properties or len(properties) == 0:
        raise Exception("No properties to analyze")
    
    prices = [p['price'] for p in properties if p.get('price', 0) > 0]
    prices_per_sqft = [p['price_per_sqft'] for p in properties if p.get('price_per_sqft', 0) > 0]
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
        
        # Generate AI intelligence
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
                'ai_model': 'gpt-4o'
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
