"""
VOXMILL ELITE PDF GENERATOR — UNIFIED BRAND EDITION
====================================================
Cinematic black/bronze/gold market intelligence reports
Matches elite email design aesthetic
Goldman Sachs x McKinsey visual authority
"""

import os
import json
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_JUSTIFY
from reportlab.pdfgen import canvas
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

INPUT_FILE = "/tmp/voxmill_analysis.json"
OUTPUT_FILE = "/tmp/Voxmill_Elite_Intelligence.pdf"

# Voxmill Elite Brand Colors — UNIFIED WITH EMAIL DESIGN
COLOR_BLACK = colors.HexColor('#0B0B0B')           # Deep matte black (matches email)
COLOR_BRONZE = colors.HexColor('#B08D57')          # Soft metallic bronze (primary accent)
COLOR_GOLD = colors.HexColor('#CBA135')            # Refined warm gold (highlights only)
COLOR_GOLD_DARK = colors.HexColor('#967A32')       # Deep gold (for gradients/shadows)
COLOR_CARD_BG = colors.HexColor('#121212')         # Content panels (matches email box)
COLOR_CHART_BG = colors.HexColor('#1A1A1A')        # Chart backgrounds
COLOR_TABLE_ROW1 = colors.HexColor('#121212')      # Table alternating row 1
COLOR_TABLE_ROW2 = colors.HexColor('#181818')      # Table alternating row 2
COLOR_DIVIDER = colors.HexColor('#2E2E2E')         # Subtle dividers (matches email)
COLOR_TEXT_PRIMARY = colors.HexColor('#EAEAEA')    # Body text (matches email)
COLOR_TEXT_MUTED = colors.HexColor('#AFAFAF')      # Muted text (matches email)
COLOR_TEXT_FOOTER = colors.HexColor('#6B6B6B')     # Footer text (matches email)
COLOR_WHITE = colors.HexColor('#FFFFFF')

# ============================================================================
# CUSTOM PAGE TEMPLATE
# ============================================================================

class VoxmillEliteTemplate(canvas.Canvas):
    """Ultra-premium page template matching email aesthetic"""
    
    def __init__(self, *args, **kwargs):
        canvas.Canvas.__init__(self, *args, **kwargs)
        self.pages = []
        
    def showPage(self):
        self.pages.append(dict(self.__dict__))
        self._startPage()
        
    def save(self):
        page_count = len(self.pages)
        for i, page in enumerate(self.pages):
            self.__dict__.update(page)
            self.draw_page_elements(i + 1, page_count)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)
        
    def draw_page_elements(self, page_num, page_count):
        """Draw premium page elements with bronze/gold accent"""
        
        # Top bronze accent line (refined weight - matches email divider)
        self.setStrokeColor(COLOR_BRONZE)
        self.setLineWidth(2)
        self.line(0.75*inch, 10.6*inch, 7.75*inch, 10.6*inch)
        
        # Voxmill branding (top left - bronze like email)
        self.setFont('Helvetica-Bold', 11)
        self.setFillColor(COLOR_BRONZE)
        self.drawString(0.75*inch, 10.75*inch, "VOXMILL")
        
        # Subtitle (smaller, muted)
        self.setFont('Helvetica', 8)
        self.setFillColor(COLOR_TEXT_MUTED)
        self.drawString(1.45*inch, 10.76*inch, "MARKET INTELLIGENCE")
        
        # Footer elements
        self.setFont('Helvetica', 7)
        self.setFillColor(COLOR_TEXT_FOOTER)
        
        # Left: Classification
        self.drawString(0.75*inch, 0.5*inch, "CONFIDENTIAL MARKET INTELLIGENCE")
        
        # Center: Copyright
        self.drawCentredString(4.25*inch, 0.5*inch, f"© Voxmill Automations {datetime.now().year}")
        
        # Right: Page number
        self.setFont('Helvetica-Bold', 7)
        self.drawRightString(7.75*inch, 0.5*inch, f"PAGE {page_num} OF {page_count}")
        
        # Bottom divider line (matches footer style)
        self.setStrokeColor(COLOR_DIVIDER)
        self.setLineWidth(1)
        self.line(0.75*inch, 0.65*inch, 7.75*inch, 0.65*inch)

# ============================================================================
# ELITE CHART GENERATION — BRONZE/GOLD PALETTE
# ============================================================================

def create_kpi_overview_chart(data):
    """Executive KPI overview with bronze/gold cinematic aesthetic"""
    
    metrics = data['metrics']
    
    fig = plt.figure(figsize=(10, 4), facecolor='#0B0B0B')
    gs = fig.add_gridspec(1, 2, width_ratios=[1, 2], wspace=0.3)
    
    # Left: Analyzed properties bar
    ax1 = fig.add_subplot(gs[0])
    ax1.set_facecolor('#0B0B0B')
    
    ax1.bar([0], [metrics['total_properties']], width=0.6, 
            color='#B08D57', edgecolor='#967A32', linewidth=2, alpha=0.85)
    ax1.set_xlim(-0.5, 0.5)
    ax1.set_ylim(0, metrics['total_properties'] * 1.2)
    ax1.set_xticks([0])
    ax1.set_xticklabels(['Analyzed'], color='#EAEAEA', fontsize=10)
    ax1.set_ylabel('Properties', color='#EAEAEA', fontsize=10)
    ax1.tick_params(colors='#EAEAEA')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.spines['left'].set_color('#2E2E2E')
    ax1.spines['bottom'].set_color('#2E2E2E')
    ax1.grid(axis='y', alpha=0.12, color='#2E2E2E', linestyle='--', linewidth=0.5)
    
    # Right: Deal category distribution
    exceptional_deals = sum(1 for p in data['properties'] if p.get('deal_score', 0) >= 9)
    hot_deals = sum(1 for p in data['properties'] if 7 <= p.get('deal_score', 0) < 9)
    strong_value = sum(1 for p in data['properties'] if 5 <= p.get('deal_score', 0) < 7)
    
    ax2 = fig.add_subplot(gs[1])
    ax2.set_facecolor('#0B0B0B')
    
    categories = ['Exceptional\nDeals', 'Hot\nDeals', 'Strong\nValue']
    values = [exceptional_deals, hot_deals, strong_value]
    
    bars = ax2.barh(categories, values, color='#B08D57', edgecolor='#967A32', 
                    linewidth=1.5, alpha=0.8)
    
    # Add value labels in gold
    for i, (bar, val) in enumerate(zip(bars, values)):
        if val > 0:
            ax2.text(val + max(values)*0.02 if max(values) > 0 else 0.5, i, str(val), 
                    va='center', ha='left', color='#CBA135', fontsize=11, fontweight='bold')
    
    ax2.set_xlabel('Count', color='#EAEAEA', fontsize=10)
    ax2.tick_params(colors='#EAEAEA')
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.spines['left'].set_color('#2E2E2E')
    ax2.spines['bottom'].set_color('#2E2E2E')
    ax2.grid(axis='x', alpha=0.12, color='#2E2E2E', linestyle='--', linewidth=0.5)
    
    plt.tight_layout()
    
    path = '/tmp/kpi_overview.png'
    plt.savefig(path, dpi=200, bbox_inches='tight', facecolor='#0B0B0B')
    plt.close()
    
    return path

def create_performance_insights_chart(data):
    """Performance analysis with bronze/gold overlays"""
    
    properties = data['properties'][:10]
    
    fig = plt.figure(figsize=(10, 5), facecolor='#0B0B0B')
    gs = fig.add_gridspec(2, 2, hspace=0.35, wspace=0.35)
    
    # Top left: Price comparison
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.set_facecolor('#0B0B0B')
    
    prices = [p['price']/1000000 for p in properties[:6]]
    addresses = [p['address'].split(',')[0][:15] + '...' for p in properties[:6]]
    
    ax1.barh(range(len(prices)), prices, color='#B08D57', edgecolor='#967A32', 
             linewidth=1, alpha=0.75)
    ax1.set_yticks(range(len(addresses)))
    ax1.set_yticklabels(addresses, color='#EAEAEA', fontsize=8)
    ax1.set_xlabel('Price (£M)', color='#EAEAEA', fontsize=9)
    ax1.tick_params(colors='#EAEAEA', labelsize=8)
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.spines['left'].set_color('#2E2E2E')
    ax1.spines['bottom'].set_color('#2E2E2E')
    ax1.grid(axis='x', alpha=0.12, color='#2E2E2E', linewidth=0.5)
    
    # Top right: Price per sqft trend
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.set_facecolor('#0B0B0B')
    
    x = list(range(len(properties)))
    y = [p['price_per_sqft'] for p in properties]
    
    ax2.plot(x, y, color='#B08D57', linewidth=2.5, marker='o', markersize=6, 
             markerfacecolor='#CBA135', markeredgecolor='#967A32', markeredgewidth=1.5, alpha=0.9)
    ax2.fill_between(x, y, alpha=0.15, color='#B08D57')
    
    ax2.set_xlabel('Property Rank', color='#EAEAEA', fontsize=9)
    ax2.set_ylabel('£/sqft', color='#EAEAEA', fontsize=9)
    ax2.tick_params(colors='#EAEAEA', labelsize=8)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.spines['left'].set_color('#2E2E2E')
    ax2.spines['bottom'].set_color('#2E2E2E')
    ax2.grid(alpha=0.12, color='#2E2E2E', linewidth=0.5)
    
    # Bottom left: Property type distribution (pie chart)
    ax3 = fig.add_subplot(gs[1, 0])
    ax3.set_facecolor('#0B0B0B')
    
    types = {}
    for p in properties:
        ptype = p['property_type']
        types[ptype] = types.get(ptype, 0) + 1
    
    bronze_spectrum = ['#B08D57', '#CBA135', '#967A32', '#8B7209']
    wedges, texts = ax3.pie(types.values(), startangle=90, colors=bronze_spectrum[:len(types)])
    
    # Add percentage labels
    total = sum(types.values())
    for i, (wedge, count) in enumerate(zip(wedges, types.values())):
        angle = (wedge.theta2 + wedge.theta1) / 2
        x = 0.7 * np.cos(np.radians(angle))
        y = 0.7 * np.sin(np.radians(angle))
        pct = int(count/total*100) if total > 0 else 0
        ax3.text(x, y, f'{pct}%', ha='center', va='center',
                color='#0B0B0B', fontsize=11, fontweight='bold')
    
    ax3.legend(types.keys(), loc='lower left', fontsize=7, 
              facecolor='#121212', edgecolor='#2E2E2E', labelcolor='#EAEAEA')
    
    # Bottom right: Deal score distribution
    ax4 = fig.add_subplot(gs[1, 1])
    ax4.set_facecolor('#0B0B0B')
    
    scores = [p['deal_score'] for p in properties]
    colors_map = ['#CBA135' if s >= 8 else '#B08D57' if s >= 7 else '#967A32' for s in scores]
    
    ax4.bar(range(len(scores)), scores, color=colors_map, edgecolor='#2E2E2E', 
            linewidth=1, alpha=0.8)
    ax4.axhline(y=8, color='#CBA135', linestyle='--', linewidth=1, alpha=0.35)
    ax4.axhline(y=7, color='#B08D57', linestyle='--', linewidth=1, alpha=0.35)
    
    ax4.set_xlabel('Property', color='#EAEAEA', fontsize=9)
    ax4.set_ylabel('Deal Score', color='#EAEAEA', fontsize=9)
    ax4.set_ylim(0, 10)
    ax4.tick_params(colors='#EAEAEA', labelsize=8)
    ax4.spines['top'].set_visible(False)
    ax4.spines['right'].set_visible(False)
    ax4.spines['left'].set_color('#2E2E2E')
    ax4.spines['bottom'].set_color('#2E2E2E')
    ax4.grid(axis='y', alpha=0.12, color='#2E2E2E', linewidth=0.5)
    
    plt.tight_layout()
    
    path = '/tmp/performance_insights.png'
    plt.savefig(path, dpi=200, bbox_inches='tight', facecolor='#0B0B0B')
    plt.close()
    
    return path

def create_competitor_landscape_chart(data):
    """Competitor analysis with bronze/gold palette"""
    
    properties = data['properties']
    
    # Get unique agents
    agents = {}
    for p in properties:
        agent = p.get('agent', 'Private')[:20]
        agents[agent] = agents.get(agent, 0) + 1
    
    # Top 5 agents
    top_agents = sorted(agents.items(), key=lambda x: x[1], reverse=True)[:5]
    
    fig = plt.figure(figsize=(10, 4), facecolor='#0B0B0B')
    gs = fig.add_gridspec(1, 2, width_ratios=[1.2, 1], wspace=0.35)
    
    # Left: Agent listings bar chart
    ax1 = fig.add_subplot(gs[0])
    ax1.set_facecolor('#0B0B0B')
    
    agent_names = [a[0] for a in top_agents]
    agent_counts = [a[1] for a in top_agents]
    
    bars = ax1.barh(range(len(agent_names)), agent_counts, color='#B08D57', 
                    edgecolor='#967A32', linewidth=1.5, alpha=0.75)
    
    for i, (bar, count) in enumerate(zip(bars, agent_counts)):
        if count > 0:
            ax1.text(count + max(agent_counts)*0.02, i, str(count), va='center', ha='left',
                    color='#CBA135', fontsize=10, fontweight='bold')
    
    ax1.set_yticks(range(len(agent_names)))
    ax1.set_yticklabels(agent_names, color='#EAEAEA', fontsize=9)
    ax1.set_xlabel('Listings', color='#EAEAEA', fontsize=9)
    ax1.tick_params(colors='#EAEAEA')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.spines['left'].set_color('#2E2E2E')
    ax1.spines['bottom'].set_color('#2E2E2E')
    ax1.grid(axis='x', alpha=0.12, color='#2E2E2E', linewidth=0.5)
    
    # Right: Market share pie
    ax2 = fig.add_subplot(gs[1])
    ax2.set_facecolor('#0B0B0B')
    
    colors_pie = ['#B08D57', '#CBA135', '#967A32', '#8B7209', '#6B5507']
    wedges, texts = ax2.pie(agent_counts, startangle=90, colors=colors_pie[:len(agent_counts)])
    
    total = sum(agent_counts)
    for i, (wedge, count) in enumerate(zip(wedges, agent_counts)):
        angle = (wedge.theta2 + wedge.theta1) / 2
        x = 0.7 * np.cos(np.radians(angle))
        y = 0.7 * np.sin(np.radians(angle))
        pct = int(count/total*100) if total > 0 else 0
        ax2.text(x, y, f'{pct}%', ha='center', va='center',
                color='#0B0B0B', fontsize=11, fontweight='bold')
    
    plt.tight_layout()
    
    path = '/tmp/competitor_landscape.png'
    plt.savefig(path, dpi=200, bbox_inches='tight', facecolor='#0B0B0B')
    plt.close()
    
    return path

# ============================================================================
# PDF CONSTRUCTION
# ============================================================================

def create_elite_pdf(data):
    """Create the elite PDF report matching email aesthetic"""
    
    print(f"\n📄 CREATING ELITE PDF (UNIFIED BRAND)")
    
    doc = SimpleDocTemplate(
        OUTPUT_FILE,
        pagesize=letter,
        leftMargin=0.75*inch,
        rightMargin=0.75*inch,
        topMargin=1.1*inch,
        bottomMargin=0.9*inch
    )
    
    story = []
    styles = getSampleStyleSheet()
    
    # Custom styles matching email typography
    title_style = ParagraphStyle(
        'EliteTitle',
        parent=styles['Heading1'],
        fontSize=32,
        textColor=COLOR_WHITE,
        spaceAfter=10,
        fontName='Helvetica-Bold',
        leading=36
    )
    
    subtitle_style = ParagraphStyle(
        'EliteSubtitle',
        parent=styles['Normal'],
        fontSize=11,
        textColor=COLOR_BRONZE,  # Bronze instead of gold
        spaceAfter=25,
        fontName='Helvetica-Bold'
    )
    
    section_title_style = ParagraphStyle(
        'SectionTitle',
        parent=styles['Heading2'],
        fontSize=18,
        textColor=COLOR_WHITE,
        spaceBefore=12,
        spaceAfter=18,
        fontName='Helvetica-Bold',
        leading=22
    )
    
    body_style = ParagraphStyle(
        'EliteBody',
        parent=styles['Normal'],
        fontSize=10,
        textColor=COLOR_TEXT_PRIMARY,  # Matches email body text
        spaceAfter=12,
        leading=15,
        alignment=TA_JUSTIFY
    )
    
    # ========================================================================
    # PAGE 1: KPI SUMMARY OVERVIEW
    # ========================================================================
    
    metadata = data['metadata']
    metrics = data['metrics']
    intelligence = data['intelligence']
    
    story.append(Paragraph("KPI SUMMARY", section_title_style))
    story.append(Paragraph("OVERVIEW", title_style))
    story.append(Spacer(1, 0.2*inch))
    
    # Market info
    info_text = f"""EXECUTIVE KPI OVERVIEW — WEEKLY MARKET PRECISION REPORT<br/>
    <b>{metadata['area'].upper()}, {metadata['city'].upper()}</b> | {datetime.now().strftime('%B %d, %Y')}"""
    
    story.append(Paragraph(info_text, subtitle_style))
    story.append(Spacer(1, 0.15*inch))
    
    # KPI chart
    print(f"   → Generating KPI overview chart...")
    kpi_chart = create_kpi_overview_chart(data)
    story.append(Image(kpi_chart, width=6.5*inch, height=2.6*inch))
    
    story.append(Spacer(1, 0.3*inch))
    
    # Pricing summary table with bronze header
    pricing_data = [
        ['', 'LOW VALUE', 'HIGH VALUE', 'AVG VALUE'],
        ['Price', f'£{metrics["min_price"]:,}', f'£{metrics["max_price"]:,}', f'£{metrics["avg_price"]:,.0f}'],
        ['Description', 'Entry-level market\npricing benchmark', 
         'Upper luxury market\nsegment ceiling', 'Central market trend\nindicator']
    ]
    
    pricing_table = Table(pricing_data, colWidths=[1.2*inch, 1.8*inch, 1.8*inch, 1.8*inch])
    pricing_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), COLOR_BRONZE),  # Bronze header
        ('BACKGROUND', (0, 1), (0, 1), COLOR_CARD_BG),
        ('BACKGROUND', (1, 1), (-1, 1), COLOR_TABLE_ROW1),
        ('BACKGROUND', (0, 2), (0, 2), COLOR_CARD_BG),
        ('BACKGROUND', (1, 2), (-1, 2), COLOR_TABLE_ROW2),
        ('TEXTCOLOR', (0, 0), (-1, 0), COLOR_WHITE),
        ('TEXTCOLOR', (0, 1), (0, -1), COLOR_GOLD),  # Gold accent
        ('TEXTCOLOR', (1, 1), (-1, -1), COLOR_TEXT_PRIMARY),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
        ('TOPPADDING', (0, 0), (-1, -1), 12),
        ('GRID', (0, 0), (-1, -1), 0.5, COLOR_DIVIDER),
        ('LINEABOVE', (0, 0), (-1, 0), 2, COLOR_BRONZE),
    ]))
    
    story.append(pricing_table)
    
    story.append(PageBreak())
    
    # ========================================================================
    # PAGE 2: PERFORMANCE INSIGHTS
    # ========================================================================
    
    story.append(Paragraph("PERFORMANCE GRAPH", section_title_style))
    story.append(Paragraph("AND MARKET INSIGHTS", title_style))
    story.append(Spacer(1, 0.2*inch))
    
    # Performance charts
    print(f"   → Generating performance insights chart...")
    perf_chart = create_performance_insights_chart(data)
    story.append(Image(perf_chart, width=6.5*inch, height=3.25*inch))
    
    story.append(Spacer(1, 0.25*inch))
    
    # Key insights
    story.append(Paragraph("KEY INSIGHTS", section_title_style))
    
    insights_text = intelligence.get('executive_summary', 'Market analysis in progress.')
    story.append(Paragraph(insights_text, body_style))
    
    if 'strategic_insights' in intelligence:
        for insight in intelligence['strategic_insights'][:3]:
            story.append(Paragraph(f"• {insight}", body_style))
    
    story.append(PageBreak())
    
    # ========================================================================
    # PAGE 3: COMPETITOR LANDSCAPE
    # ========================================================================
    
    story.append(Paragraph("COMPETITOR", section_title_style))
    story.append(Paragraph("<font color='#B08D57'>LANDSCAPE</font>", title_style))  # Bronze
    story.append(Paragraph("ANALYSIS", title_style))
    story.append(Spacer(1, 0.2*inch))
    
    # Competitor chart
    print(f"   → Generating competitor landscape chart...")
    comp_chart = create_competitor_landscape_chart(data)
    story.append(Image(comp_chart, width=6.5*inch, height=2.6*inch))
    
    story.append(Spacer(1, 0.25*inch))
    
    # Competitor insights
    insight_text = """Competitors are actively adjusting strategies to capture market share. 
    Price reductions and new listings are prevalent across major agencies."""
    
    story.append(Paragraph(insight_text, body_style))
    
    story.append(PageBreak())
    
    # ========================================================================
    # PAGE 4: STRATEGIC INTELLIGENCE
    # ========================================================================
    
    story.append(Paragraph("STRATEGIC INTELLIGENCE", title_style))
    story.append(Spacer(1, 0.3*inch))
    
    # Executive Summary
    story.append(Paragraph("EXECUTIVE SUMMARY", section_title_style))
    bluf_text = intelligence.get('executive_summary', 'Analysis based on current market conditions.')
    story.append(Paragraph(bluf_text, body_style))
    
    story.append(Spacer(1, 0.2*inch))
    
    # Tactical Opportunities
    if 'tactical_opportunities' in intelligence:
        story.append(Paragraph("TACTICAL OPPORTUNITIES", section_title_style))
        
        tact_opps = intelligence['tactical_opportunities']
        if isinstance(tact_opps, dict):
            if 'immediate' in tact_opps:
                story.append(Paragraph(f"<b>Immediate:</b> {tact_opps['immediate']}", body_style))
            if 'near_term' in tact_opps:
                story.append(Paragraph(f"<b>Near-term:</b> {tact_opps['near_term']}", body_style))
            if 'strategic' in tact_opps:
                story.append(Paragraph(f"<b>Strategic:</b> {tact_opps['strategic']}", body_style))
        else:
            story.append(Paragraph(str(tact_opps), body_style))
    
    story.append(Spacer(1, 0.2*inch))
    
    # Risk Assessment
    story.append(Paragraph("RISK ASSESSMENT", section_title_style))
    risk_text = intelligence.get('risk_assessment', 'Standard market risks apply.')
    story.append(Paragraph(risk_text, body_style))
    
    story.append(Spacer(1, 0.2*inch))
    
    # Market Sentiment
    sentiment = intelligence.get('market_sentiment', 'Neutral')
    confidence = intelligence.get('confidence_level', 'Medium')
    
    sentiment_text = f"<b>Market Sentiment:</b> {sentiment} | <b>Confidence Level:</b> {confidence}"
    story.append(Paragraph(sentiment_text, subtitle_style))
    
    story.append(PageBreak())
    
    # ========================================================================
    # PAGE 5: TOP OPPORTUNITIES
    # ========================================================================
    
    story.append(Paragraph("TOP OPPORTUNITIES", title_style))
    story.append(Spacer(1, 0.25*inch))
    
    # Top 8 properties table
    top_props = data['top_opportunities'][:8]
    
    prop_data = [['ADDRESS', 'PRICE', 'BEDS/BATHS', '£/SQFT', 'SCORE']]
    
    for prop in top_props:
        prop_data.append([
            prop['address'].split(',')[0][:32],
            f"£{prop['price']:,}",
            f"{prop['beds']}/{prop['baths']}",
            f"£{prop['price_per_sqft']:,.0f}",
            f"{prop['deal_score']}/10"
        ])
    
    prop_table = Table(prop_data, colWidths=[2.3*inch, 1.3*inch, 0.9*inch, 0.9*inch, 0.7*inch])
    prop_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), COLOR_BRONZE),  # Bronze header
        ('BACKGROUND', (0, 1), (-1, -1), COLOR_TABLE_ROW1),
        ('TEXTCOLOR', (0, 0), (-1, 0), COLOR_WHITE),
        ('TEXTCOLOR', (0, 1), (-1, -1), COLOR_TEXT_PRIMARY),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('ALIGN', (1, 1), (-1, -1), 'CENTER'),
        ('ALIGN', (0, 1), (0, -1), 'LEFT'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('FONTSIZE', (0, 1), (-1, -1), 8.5),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ('TOPPADDING', (0, 0), (-1, -1), 10),
        ('GRID', (0, 0), (-1, -1), 0.5, COLOR_DIVIDER),
        ('LINEABOVE', (0, 0), (-1, 0), 2, COLOR_BRONZE),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [COLOR_TABLE_ROW1, COLOR_TABLE_ROW2])
    ]))
    
    story.append(prop_table)
    
    story.append(Spacer(1, 0.35*inch))
    
    # Closing statement
    closing_text = """<b>INSIGHTS SUMMARY</b><br/><br/>
    Key insights highlighted above represent immediate market opportunities based on current data analysis. 
    This intelligence is designed to support strategic decision-making and competitive positioning in the luxury real estate market."""
    
    story.append(Paragraph(closing_text, body_style))
    
    # Build PDF
    print(f"   → Building PDF with unified brand template...")
    doc.build(story, canvasmaker=VoxmillEliteTemplate)
    
    print(f"   ✅ Elite PDF created: {OUTPUT_FILE}")
    return OUTPUT_FILE

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main execution"""
    
    print("\n" + "="*70)
    print("VOXMILL ELITE PDF GENERATOR — UNIFIED BRAND EDITION")
    print("="*70)
    
    try:
        if not os.path.exists(INPUT_FILE):
            raise Exception(f"Intelligence data not found: {INPUT_FILE}")
        
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        pdf_file = create_elite_pdf(data)
        
        print(f"\n✅ PDF generation complete")
        print(f"   Output: {pdf_file}")
        return pdf_file
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
