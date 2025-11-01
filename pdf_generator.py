"""
VOXMILL EXECUTIVE PDF GENERATOR - HTML/CSS EDITION
===================================================
Boardroom-quality PDF via WeasyPrint
Goldman Sachs / McKinsey aesthetic
"""

import os
import json
import math
from datetime import datetime
from pathlib import Path
from jinja2 import Template

try:
    from weasyprint import HTML, CSS
    WEASY_AVAILABLE = True
except ImportError:
    print("⚠️  WeasyPrint not available. Install: pip install weasyprint")
    WEASY_AVAILABLE = False

INPUT_FILE = "/tmp/voxmill_analysis.json"
OUTPUT_FILE = "/tmp/Voxmill_Executive_Intelligence.pdf"
TEMPLATE_DIR = Path(__file__).parent
HTML_TEMPLATE = TEMPLATE_DIR / "voxmill_report.html"
CSS_FILE = TEMPLATE_DIR / "voxmill_style.css"

def format_number(value):
    """Format number with commas"""
    try:
        return f"{int(value):,}"
    except:
        return str(value)

def truncate_text(text, length=35):
    """Truncate text with ellipsis"""
    return text if len(text) <= length else text[:length-3] + "..."

def calculate_competitors(properties):
    """Extract top 5 competitors from properties"""
    agents = {}
    for prop in properties:
        agent = prop.get('agent', 'Private')[:20]
        agents[agent] = agents.get(agent, 0) + 1
    
    sorted_agents = sorted(agents.items(), key=lambda x: x[1], reverse=True)[:5]
    total = sum(a[1] for a in sorted_agents)
    
    return [
        {
            'name': agent,
            'count': count,
            'share': count / total if total > 0 else 0
        }
        for agent, count in sorted_agents
    ]

def prepare_chart_data(data):
    """Prepare data for SVG charts"""
    properties = data['properties']
    
    exceptional = sum(1 for p in properties if p.get('deal_score', 0) >= 9)
    hot = sum(1 for p in properties if 7 <= p.get('deal_score', 0) < 9)
    strong = sum(1 for p in properties if 5 <= p.get('deal_score', 0) < 7)
    
    return {
        'deal_tiers': [exceptional, hot, strong]
    }

def enrich_intelligence(intel):
    """Add default intelligence sections if missing"""
    return {
        'executive_summary': intel.get('executive_summary', 'Market analysis indicates strong momentum with selective premium opportunities across the evaluated properties.'),
        'market_velocity': intel.get('market_velocity', 'High activity with accelerated transaction timelines'),
        'deal_quality': intel.get('deal_quality', 'Premium positioning with strong fundamentals'),
        'risk_level': intel.get('risk_level', 'Moderate — diversified exposure recommended'),
        'market_dynamics': intel.get('market_dynamics', 'The current market environment demonstrates robust demand dynamics with selective supply constraints driving premium valuations in core locations.'),
        'recommendations': intel.get('recommendations', 'Focus on properties with exceptional deal scores (9+) and strong price-per-square-foot metrics relative to comparable transactions.')
    }

def generate_html_pdf(data):
    """Generate executive PDF using WeasyPrint"""
    
    print("\n" + "="*70)
    print("VOXMILL EXECUTIVE PDF GENERATOR - HTML EDITION")
    print("="*70)
    
    if not WEASY_AVAILABLE:
        raise ImportError("WeasyPrint is required. Install with: pip install weasyprint")
    
    # Extract data
    metadata = data['metadata']
    metrics = data['metrics']
    intelligence = enrich_intelligence(data['intelligence'])
    properties = data['properties']
    top_opportunities = data['top_opportunities'][:8]
    
    # Calculate additional metrics
    metrics_enriched = {
        **metrics,
        'avg_price_millions': round(metrics['avg_price'] / 1_000_000, 1),
        'exceptional_deals': sum(1 for p in properties if p.get('deal_score', 0) >= 9),
        'hot_deals': sum(1 for p in properties if 7 <= p.get('deal_score', 0) < 9)
    }
    
    # Prepare top properties with formatted data
    top_properties_formatted = [
        {
            **prop,
            'price_millions': round(prop['price'] / 1_000_000, 1)
        }
        for prop in properties[:10]
    ]
    
    # Prepare opportunities with formatted data
    opportunities_formatted = [
        {
            'address': truncate_text(opp['address'].split(',')[0], 35),
            'price': opp['price'],
            'beds': opp.get('beds', '-'),
            'baths': opp.get('baths', '-'),
            'price_per_sqft': round(opp.get('price_per_sqft', 0)),
            'deal_score': opp.get('deal_score', 0)
        }
        for opp in top_opportunities
    ]
    
    # Calculate competitors
    competitors = calculate_competitors(properties)
    
    # Prepare chart data
    chart_data = prepare_chart_data(data)
    
    # Template context
    context = {
        'area': metadata['area'],
        'city': metadata['city'],
        'date': datetime.now().strftime('%B %Y'),
        'year': datetime.now().year,
        'metrics': metrics_enriched,
        'intelligence': intelligence,
        'top_properties': top_properties_formatted,
        'top_opportunities': opportunities_formatted,
        'competitors': competitors,
        'chart_data': chart_data,
        # Helper functions
        'enumerate': enumerate,
        'range': range,
        'cos': math.cos,
        'sin': math.sin,
        'round': round,
        'int': int,
        'format_number': format_number,
        'truncate': truncate_text,
        'sum': sum,
    }
    
    # Load HTML template
    print("→ Loading HTML template...")
    if not HTML_TEMPLATE.exists():
        raise FileNotFoundError(f"Template not found: {HTML_TEMPLATE}")
    
    with open(HTML_TEMPLATE, 'r', encoding='utf-8') as f:
        template_content = f.read()
    
    # Render template
    print("→ Rendering Jinja2 template...")
    template = Template(template_content)
    html_content = template.render(**context)
    
    # Load CSS
    print("→ Loading CSS stylesheet...")
    if not CSS_FILE.exists():
        print(f"⚠️  CSS file not found: {CSS_FILE}")
        css_content = None
    else:
        with open(CSS_FILE, 'r', encoding='utf-8') as f:
            css_content = f.read()
    
    # Generate PDF
    print("→ Generating PDF with WeasyPrint...")
    html_obj = HTML(string=html_content, base_url=str(TEMPLATE_DIR))
    
    if css_content:
        css_obj = CSS(string=css_content)
        html_obj.write_pdf(OUTPUT_FILE, stylesheets=[css_obj])
    else:
        html_obj.write_pdf(OUTPUT_FILE)
    
    file_size = Path(OUTPUT_FILE).stat().st_size
    print(f"✅ PDF created: {OUTPUT_FILE}")
    print(f"   Size: {file_size:,} bytes ({file_size/1024:.1f} KB)")
    print("="*70 + "\n")
    
    return OUTPUT_FILE

def main():
    """Main execution"""
    try:
        # Load data
        if not os.path.exists(INPUT_FILE):
            raise Exception(f"Data not found: {INPUT_FILE}")
        
        print(f"→ Loading data from {INPUT_FILE}...")
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # Generate PDF
        pdf_file = generate_html_pdf(data)
        
        return pdf_file
        
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
