"""
VOXMILL ELITE AI ANALYZER
==========================
GPT-4o powered market intelligence engine
Anomaly detection, trend identification, executive insights

MAXIMUM AI UTILIZATION. ZERO GENERIC OUTPUTS.
"""

import os
import json
from datetime import datetime
from openai import OpenAI

OPENAI_API_KEY = os.environ.get('OPENAI_API_KEY')
INPUT_FILE = "/tmp/voxmill_raw_data.json"
OUTPUT_FILE = "/tmp/voxmill_intelligence.json"

# ============================================================================
# MARKET METRICS CALCULATION
# ============================================================================

def calculate_metrics(properties):
    """Calculate advanced market metrics"""
    
    print(f"\n📊 CALCULATING MARKET METRICS")
    
    if not properties or len(properties) < 2:
        raise Exception("Insufficient properties for analysis (need at least 2)")
    
    prices = [p['price'] for p in properties]
    ppsf_values = [p['price_per_sqft'] for p in properties if p['price_per_sqft'] > 0]
    
    # Core statistics
    avg_price = int(sum(prices) / len(prices))
    median_price = int(sorted(prices)[len(prices) // 2])
    min_price = min(prices)
    max_price = max(prices)
    
    avg_ppsf = int(sum(ppsf_values) / len(ppsf_values)) if ppsf_values else 0
    median_ppsf = int(sorted(ppsf_values)[len(ppsf_values) // 2]) if ppsf_values else 0
    
    # Advanced metrics
    price_std_dev = calculate_std_dev(prices)
    ppsf_std_dev = calculate_std_dev(ppsf_values) if ppsf_values else 0
    
    # Deal scoring
    for prop in properties:
        prop['deal_score'] = score_property(prop, avg_ppsf, median_ppsf)
        prop['anomaly_flags'] = detect_anomalies(prop, avg_price, avg_ppsf, price_std_dev, ppsf_std_dev)
    
    # Sort by deal score
    properties.sort(key=lambda x: x['deal_score'], reverse=True)
    
    # Count tiers
    exceptional = len([p for p in properties if p['deal_score'] >= 9.0])
    hot_deals = len([p for p in properties if p['deal_score'] >= 8.0])
    strong_value = len([p for p in properties if p['deal_score'] >= 7.0])
    
    print(f"   → Avg Price: £{avg_price:,}")
    print(f"   → Avg £/sqft: £{avg_ppsf:,}")
    print(f"   → Exceptional: {exceptional}, Hot: {hot_deals}, Strong: {strong_value}")
    
    return {
        'total_properties': len(properties),
        'avg_price': avg_price,
        'median_price': median_price,
        'min_price': min_price,
        'max_price': max_price,
        'price_std_dev': price_std_dev,
        'avg_ppsf': avg_ppsf,
        'median_ppsf': median_ppsf,
        'ppsf_std_dev': ppsf_std_dev,
        'exceptional_deals': exceptional,
        'hot_deals': hot_deals,
        'strong_value': strong_value,
        'properties': properties
    }

def calculate_std_dev(values):
    """Calculate standard deviation"""
    if not values:
        return 0
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    return int(variance ** 0.5)

def score_property(prop, avg_ppsf, median_ppsf):
    """Advanced property scoring algorithm"""
    
    score = 5.0  # Base score
    ppsf = prop['price_per_sqft']
    
    if ppsf > 0 and avg_ppsf > 0:
        ratio = ppsf / avg_ppsf
        
        # Price/sqft scoring (heavily weighted)
        if ratio < 0.70:
            score += 4.0  # Exceptional value
        elif ratio < 0.80:
            score += 3.0  # Very strong value
        elif ratio < 0.90:
            score += 2.0  # Strong value
        elif ratio < 0.95:
            score += 1.0  # Good value
        elif ratio > 1.30:
            score -= 3.0  # Significantly overpriced
        elif ratio > 1.20:
            score -= 2.0  # Overpriced
        elif ratio > 1.10:
            score -= 1.0  # Slightly overpriced
    
    # Size bonus (larger properties are rarer)
    if prop['beds'] >= 6:
        score += 1.0
    elif prop['beds'] >= 5:
        score += 0.5
    
    # Sqft bonus
    if prop['sqft'] >= 4000:
        score += 0.5
    
    return round(min(max(score, 1.0), 10.0), 1)

def detect_anomalies(prop, avg_price, avg_ppsf, price_std_dev, ppsf_std_dev):
    """Detect pricing anomalies"""
    
    flags = []
    
    # Price anomalies
    if prop['price'] < avg_price - (2 * price_std_dev):
        flags.append("SIGNIFICANT_UNDERPRICING")
    elif prop['price'] > avg_price + (2 * price_std_dev):
        flags.append("SIGNIFICANT_OVERPRICING")
    
    # $/sqft anomalies
    if prop['price_per_sqft'] > 0 and avg_ppsf > 0:
        if prop['price_per_sqft'] < avg_ppsf - (2 * ppsf_std_dev):
            flags.append("PPSF_ANOMALY_LOW")
        elif prop['price_per_sqft'] > avg_ppsf + (2 * ppsf_std_dev):
            flags.append("PPSF_ANOMALY_HIGH")
    
    # Value anomalies
    if prop['deal_score'] >= 9.0:
        flags.append("EXCEPTIONAL_OPPORTUNITY")
    
    return flags

# ============================================================================
# GPT-4O AI INTELLIGENCE GENERATION
# ============================================================================

def generate_ai_intelligence(metrics, area, city):
    """Generate elite AI-powered market intelligence"""
    
    print(f"\n🧠 GENERATING AI INTELLIGENCE (GPT-4o)")
    
    if not OPENAI_API_KEY:
        raise Exception("OPENAI_API_KEY not configured")
    
    client = OpenAI(api_key=OPENAI_API_KEY)
    
    # Prepare context
    top_10 = metrics['properties'][:10]
    anomalies = [p for p in metrics['properties'] if p['anomaly_flags']][:5]
    
    context = f"""ULTRA-PREMIUM MARKET INTELLIGENCE ANALYSIS

LOCATION: {city} - {area}
ANALYSIS DATE: {datetime.now().strftime("%B %d, %Y")}

MARKET SNAPSHOT:
• Total Properties Analyzed: {metrics['total_properties']}
• Price Range: £{metrics['min_price']:,} - £{metrics['max_price']:,}
• Average Price: £{metrics['avg_price']:,}
• Median Price: £{metrics['median_price']:,}
• Price Volatility (σ): £{metrics['price_std_dev']:,}
• Average £/sqft: £{metrics['avg_ppsf']:,}
• Median £/sqft: £{metrics['median_ppsf']:,}
• £/sqft Volatility (σ): £{metrics['ppsf_std_dev']:,}

DEAL CLASSIFICATION:
• Exceptional Opportunities (9.0+ score): {metrics['exceptional_deals']}
• Hot Deals (8.0-8.9 score): {metrics['hot_deals']}
• Strong Value (7.0-7.9 score): {metrics['strong_value']}

TOP 10 PROPERTIES:
""" + "\n".join([
    f"{i+1}. {p['address'][:40]} | £{p['price']:,} | {p['beds']}bd/{p['baths']}ba | {p['sqft']} sqft | £{p['price_per_sqft']:,}/sqft | Score: {p['deal_score']}/10"
    for i, p in enumerate(top_10)
]) + f"""

DETECTED ANOMALIES:
""" + ("\n".join([
    f"• {p['address'][:40]} | Flags: {', '.join(p['anomaly_flags'])}"
    for p in anomalies
]) if anomalies else "• No significant anomalies detected")
    
    intelligence = {}
    
    # 1. Executive BLUF
    print(f"   → BLUF generation...")
    bluf_prompt = f"""{context}

Generate a MILITARY-STYLE BLUF (Bottom Line Up Front) in EXACTLY 3 lines. This is for C-level executives at luxury real estate agencies. Be direct, data-driven, and actionable.

Format:
INSIGHT: [One critical sentence about the market state RIGHT NOW]
ACTION: [One sentence: What specific action should be taken THIS WEEK]
RISK: [One sentence: What is the biggest immediate threat]

Use specific numbers. No fluff. Pure intelligence."""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a senior market intelligence analyst for ultra-high-net-worth real estate firms. Your reports drive 8-figure decisions."},
            {"role": "user", "content": bluf_prompt}
        ],
        temperature=0.5,
        max_tokens=200
    )
    intelligence['bluf'] = response.choices[0].message.content.strip()
    print(f"   ✅ BLUF complete")
    
    # 2. Anomaly Deep Dive
    print(f"   → Anomaly analysis...")
    anomaly_prompt = f"""{context}

Analyze the detected anomalies. For each significant anomaly, explain:
1. WHY it exists (market mechanics, not speculation)
2. WHAT action should be taken
3. RISK of not acting

Focus on the top 3 most significant anomalies. Be clinical and precise."""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a quantitative analyst identifying market inefficiencies."},
            {"role": "user", "content": anomaly_prompt}
        ],
        temperature=0.6,
        max_tokens=400
    )
    intelligence['anomaly_analysis'] = response.choices[0].message.content.strip()
    print(f"   ✅ Anomaly analysis complete")
    
    # 3. Strategic Opportunities
    print(f"   → Opportunity identification...")
    opps_prompt = f"""{context}

Identify the 3 HIGHEST-ROI opportunities in this market, ranked by urgency:

1. IMMEDIATE (act within 7 days): [Specific property or strategy with exact £ amount and expected ROI]
2. TACTICAL (30-day window): [Market positioning move with quantified impact]
3. STRATEGIC (90-day horizon): [Trend to capitalize on with projected value]

Include specific property addresses and pricing where relevant."""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are an investment strategist specializing in luxury real estate alpha generation."},
            {"role": "user", "content": opps_prompt}
        ],
        temperature=0.7,
        max_tokens=400
    )
    intelligence['opportunities'] = response.choices[0].message.content.strip()
    print(f"   ✅ Opportunities complete")
    
    # 4. Risk Assessment
    print(f"   → Risk assessment...")
    risk_prompt = f"""{context}

Conduct a risk assessment. Identify 3 critical risks:

PRICING RISK: [Specific price movements that threaten deal value - use exact £/sqft thresholds]
MARKET RISK: [Supply/demand imbalances - quantify with specific metrics]
EXECUTION RISK: [Competitive threats or timing issues - be specific about who/what]

Focus on quantifiable, actionable risks."""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a risk analyst for institutional real estate investors."},
            {"role": "user", "content": risk_prompt}
        ],
        temperature=0.6,
        max_tokens=350
    )
    intelligence['risks'] = response.choices[0].message.content.strip()
    print(f"   ✅ Risk assessment complete")
    
    # 5. Market Trends
    print(f"   → Trend detection...")
    trend_prompt = f"""{context}

Based on price distribution, volatility, and property characteristics, identify:

1. DOMINANT TREND: [What's the primary market movement?]
2. EMERGING PATTERN: [What's developing that isn't obvious yet?]
3. INFLECTION SIGNAL: [What metric suggests the market is about to shift?]

Use statistical evidence from the data."""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a quantitative market researcher identifying trend patterns."},
            {"role": "user", "content": trend_prompt}
        ],
        temperature=0.6,
        max_tokens=300
    )
    intelligence['trends'] = response.choices[0].message.content.strip()
    print(f"   ✅ Trend detection complete")
    
    # 6. Action Triggers (for live alerts)
    print(f"   → Generating action triggers...")
    trigger_prompt = f"""Based on these metrics:
- Exceptional deals: {metrics['exceptional_deals']}
- Avg £/sqft: £{metrics['avg_ppsf']:,}
- Price volatility: £{metrics['price_std_dev']:,}

Create 3 IF-THEN action triggers for automated monitoring:

IF [specific measurable condition with exact numbers] THEN [specific tactical action]

Example: IF exceptional deals drop below 3 THEN increase acquisition budget 25% and accelerate due diligence

Make them precise and automatable."""
    
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "You are a trading system architect creating decision rules."},
            {"role": "user", "content": trigger_prompt}
        ],
        temperature=0.7,
        max_tokens=250
    )
    intelligence['action_triggers'] = response.choices[0].message.content.strip()
    print(f"   ✅ Action triggers complete")
    
    # 7. Alert Detection
    alert_worthy = []
    if metrics['exceptional_deals'] >= 5:
        alert_worthy.append(f"HIGH_OPPORTUNITY_VOLUME: {metrics['exceptional_deals']} exceptional deals available")
    if anomalies:
        alert_worthy.append(f"PRICING_ANOMALIES: {len(anomalies)} properties flagged")
    if metrics['ppsf_std_dev'] > metrics['avg_ppsf'] * 0.3:
        alert_worthy.append(f"HIGH_VOLATILITY: £/sqft std dev is {int((metrics['ppsf_std_dev']/metrics['avg_ppsf'])*100)}% of mean")
    
    intelligence['alert_flags'] = alert_worthy
    
    print(f"   ✅ AI intelligence generation complete")
    return intelligence

# ============================================================================
# MAIN ANALYZER
# ============================================================================

def analyze_market_data():
    """Main analysis orchestrator"""
    
    print("\n" + "="*70)
    print("VOXMILL ELITE AI ANALYZER")
    print("="*70)
    
    try:
        # Load raw data
        if not os.path.exists(INPUT_FILE):
            raise Exception(f"Raw data not found: {INPUT_FILE}")
        
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)
        
        metadata = raw_data['metadata']
        properties = raw_data['raw_data'].get('properties', [])
        
        if not properties:
            raise Exception("No properties in raw data")
        
        # Calculate metrics
        metrics = calculate_metrics(properties)
        
        # Generate AI intelligence
        intelligence = generate_ai_intelligence(
            metrics, 
            metadata['area'], 
            metadata['city']
        )
        
        # Combine everything
        output = {
            'metadata': metadata,
            'metrics': {
                'total_properties': metrics['total_properties'],
                'avg_price': metrics['avg_price'],
                'median_price': metrics['median_price'],
                'min_price': metrics['min_price'],
                'max_price': metrics['max_price'],
                'avg_ppsf': metrics['avg_ppsf'],
                'median_ppsf': metrics['median_ppsf'],
                'exceptional_deals': metrics['exceptional_deals'],
                'hot_deals': metrics['hot_deals'],
                'strong_value': metrics['strong_value']
            },
            'intelligence': intelligence,
            'properties': metrics['properties'][:20]  # Top 20 for PDF
        }
        
        # Export
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
            json.dump(output, f, indent=2, ensure_ascii=False)
        
        print(f"\n✅ Analysis complete: {OUTPUT_FILE}")
        return OUTPUT_FILE
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        raise

if __name__ == "__main__":
    analyze_market_data()
