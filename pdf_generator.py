"""
VOXMILL ELITE PDF GENERATOR
============================
Fortune 500-level visual intelligence reports
Black/gold premium design inspired by McKinsey, BCG, Goldman Sachs

THIS WILL DESTROY ANY GOOGLE SLIDES TEMPLATE.
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

# Voxmill Elite Brand Colors
COLOR_BLACK = colors.HexColor('#0A0A0A')
COLOR_GOLD = colors.HexColor('#D4AF37')
COLOR_DARK_GOLD = colors.HexColor('#B8960C')
COLOR_LIGHT_GOLD = colors.HexColor('#F4E5B8')
COLOR_GRAY_DARK = colors.HexColor('#1A1A1A')
COLOR_GRAY_MED = colors.HexColor('#4A4A4A')
COLOR_GRAY_LIGHT = colors.HexColor('#E8E8E8')
COLOR_WHITE = colors.HexColor('#FFFFFF')

# ============================================================================
# CUSTOM PAGE TEMPLATE
# ============================================================================

class VoxmillEliteTemplate(canvas.Canvas):
    """Ultra-premium page template"""
    
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
        """Draw premium page elements"""
        
        # Top gold accent line
        self.setStrokeColor(COLOR_GOLD)
        self.setLineWidth(3)
        self.line(0.5*inch, 10.7*inch, 8*inch, 10.7*inch)
        
        # Voxmill logo image (top left)
        logo_path = os.path.join(os.path.dirname(__file__), 'voxmill_logo.png')
        if os.path.exists(logo_path):
            self.drawImage(logo_path, 0.5*inch, 10.75*inch, width=0.35*inch, height=0.35*inch, preserveAspectRatio=True, mask='auto')
            # Add "VOXMILL" text next to logo
            self.setFont('Helvetica-Bold', 11)
            self.setFillColor(COLOR_GOLD)
            self.drawString(0.95*inch, 10.85*inch, "VOXMILL")
        else:
            # Fallback to text only
            self.setFont('Helvetica-Bold', 11)
            self.setFillColor(COLOR_GOLD)
            self.drawString(0.5*inch, 10.85*inch, "VOXMILL")
        
        # Footer elements
        self.setFont('Helvetica', 7)
        self.setFillColor(COLOR_GRAY_MED)
        
        # Left: Classification
        self.drawString(0.5*inch, 0.4*inch, "CONFIDENTIAL MARKET INTELLIGENCE")
        
        # Center: Copyright
        self.drawCentredString(4.25*inch, 0.4*inch, f"© Voxmill Automations {datetime.now().year}")
        
        # Right: Page number
        self.setFont('Helvetica-Bold', 7)
        self.drawRightString(8*inch, 0.4*inch, f"PAGE {page_num} OF {page_count}")
        
        # Bottom gold line
        self.setStrokeColor(COLOR_GOLD)
        self.setLineWidth(1)
        self.line(0.5*inch, 0.55*inch, 8*inch, 0.55*inch)

# ============================================================================
# ELITE CHART GENERATION
# ============================================================================

def create_kpi_overview_chart(data):
    """Create executive KPI overview (inspired by your slide 1)"""
    
    metrics = data['metrics']
    
    fig = plt.figure(figsize=(10, 4), facecolor='#0A0A0A')
    gs = fig.add_gridspec(1, 2, width_ratios=[1, 2], wspace=0.3)
    
    # Left: Simple bar (listings)
    ax1 = fig.add_subplot(gs[0])
    ax1.set_facecolor('#0A0A0A')
    
    ax1.bar([0], [metrics['total_properties']], width=0.6, 
            color='#D4AF37', edgecolor='#D4AF37', linewidth=2)
    ax1.set_xlim(-0.5, 0.5)
    ax1.set_ylim(0, metrics['total_properties'] * 1.2)
    ax1.set_xticks([0])
    ax1.set_xticklabels(['Analyzed'], color='#E8E8E8', fontsize=9)
    ax1.set_ylabel('Properties', color='#E8E8E8', fontsize=9)
    ax1.tick_params(colors='#E8E8E8')
    ax1.spines['top'].set_visible(False)
    ax1.spines('right').set_visible(False)
    ax1.spines['left'].set_color('#4A4A4A')
    ax1.spines['bottom'].set_color('#4A4A4A')
    ax1.grid(axis='y', alpha=0.2, color='#4A4A4A', linestyle='--')
    
    # Right: KPI bars
    ax2 = fig.add_subplot(gs[1])
    ax2.set_facecolor('#0A0A0A')
    
    categories = ['Exceptional\nDeals', 'Hot\nDeals', 'Strong\nValue']
    values = [metrics['exceptional_deals'], metrics['hot_deals'], metrics['strong_value']]
    
    bars = ax2.barh(categories, values, color='#D4AF37', edgecolor='#D4AF37', linewidth=1.5)
    
    # Add value labels
    for i, (bar, val) in enumerate(zip(bars, values)):
        ax2.text(val + max(values)*0.02, i, str(val), va='center', ha='left',
                color='#D4AF37', fontsize=11, fontweight='bold')
    
    ax2.set_xlabel('Count', color='#E8E8E8', fontsize=9)
    ax2.tick_params(colors='#E8E8E8')
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.spines['left'].set_color('#4A4A4A')
    ax2.spines['bottom'].set_color('#4A4A4A')
    ax2.grid(axis='x', alpha=0.2, color='#4A4A4A', linestyle='--')
    
    plt.tight_layout()
    
    path = '/tmp/kpi_overview.png'
    plt.savefig(path, dpi=200, bbox_inches='tight', facecolor='#0A0A0A')
    plt.close()
    
    return path

def create_performance_insights_chart(data):
    """Create performance graph (inspired by your slide 2)"""
    
    properties = data['properties'][:10]
    
    fig = plt.figure(figsize=(10, 5), facecolor='#0A0A0A')
    gs = fig.add_gridspec(2, 2, hspace=0.3, wspace=0.3)
    
    # Top left: Price comparison
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.set_facecolor('#0A0A0A')
    
    prices = [p['price']/1000000 for p in properties[:6]]
    addresses = [p['address'].split(',')[0][:15] + '...' for p in properties[:6]]
    
    ax1.barh(range(len(prices)), prices, color='#D4AF37', edgecolor='#B8960C', linewidth=1)
    ax1.set_yticks(range(len(addresses)))
    ax1.set_yticklabels(addresses, color='#E8E8E8', fontsize=8)
    ax1.set_xlabel('Price (£M)', color='#E8E8E8', fontsize=9)
    ax1.tick_params(colors='#E8E8E8')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.spines['left'].set_color('#4A4A4A')
    ax1.spines['bottom'].set_color('#4A4A4A')
    ax1.grid(axis='x', alpha=0.2, color='#4A4A4A')
    
    # Top right: Trend line
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.set_facecolor('#0A0A0A')
    
    x = list(range(len(properties)))
    y = [p['price_per_sqft'] for p in properties]
    
    ax2.plot(x, y, color='#D4AF37', linewidth=2.5, marker='o', markersize=6, 
             markerfacecolor='#D4AF37', markeredgecolor='#B8960C', markeredgewidth=1.5)
    ax2.fill_between(x, y, alpha=0.3, color='#B8960C')
    
    ax2.set_xlabel('Property Rank', color='#E8E8E8', fontsize=9)
    ax2.set_ylabel('£/sqft', color='#E8E8E8', fontsize=9)
    ax2.tick_params(colors='#E8E8E8')
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.spines['left'].set_color('#4A4A4A')
    ax2.spines['bottom'].set_color('#4A4A4A')
    ax2.grid(alpha=0.2, color='#4A4A4A')
    
    # Bottom left: Property type distribution
    ax3 = fig.add_subplot(gs[1, 0])
    ax3.set_facecolor('#0A0A0A')
    
    types = {}
    for p in properties:
        ptype = p['property_type']
        types[ptype] = types.get(ptype, 0) + 1
    
    wedges, texts = ax3.pie(types.values(), startangle=90, colors=['#D4AF37', '#B8960C', '#8B7209', '#6B5507'])
    
    # Add percentage labels
    total = sum(types.values())
    for i, (wedge, count) in enumerate(zip(wedges, types.values())):
        angle = (wedge.theta2 + wedge.theta1) / 2
        x = 0.7 * np.cos(np.radians(angle))
        y = 0.7 * np.sin(np.radians(angle))
        ax3.text(x, y, f'{int(count/total*100)}%', ha='center', va='center',
                color='#0A0A0A', fontsize=12, fontweight='bold')
    
    # Legend
    ax3.legend(types.keys(), loc='lower left', fontsize=7, 
              facecolor='#1A1A1A', edgecolor='#4A4A4A', labelcolor='#E8E8E8')
    
    # Bottom right: Deal score distribution
    ax4 = fig.add_subplot(gs[1, 1])
    ax4.set_facecolor('#0A0A0A')
    
    scores = [p['deal_score'] for p in properties]
    colors_map = ['#D4AF37' if s >= 8 else '#B8960C' if s >= 7 else '#6B5507' for s in scores]
    
    ax4.bar(range(len(scores)), scores, color=colors_map, edgecolor='#4A4A4A', linewidth=1)
    ax4.axhline(y=8, color='#D4AF37', linestyle='--', linewidth=1, alpha=0.5)
    ax4.axhline(y=7, color='#B8960C', linestyle='--', linewidth=1, alpha=0.5)
    
    ax4.set_xlabel('Property', color='#E8E8E8', fontsize=9)
    ax4.set_ylabel('Deal Score', color='#E8E8E8', fontsize=9)
    ax4.set_ylim(0, 10)
    ax4.tick_params(colors='#E8E8E8')
    ax4.spines['top'].set_visible(False)
    ax4.spines['right'].set_visible(False)
    ax4.spines['left'].set_color('#4A4A4A')
    ax4.spines['bottom'].set_color('#4A4A4A')
    ax4.grid(axis='y', alpha=0.2, color='#4A4A4A')
    
    plt.tight_layout()
    
    path = '/tmp/performance_insights.png'
    plt.savefig(path, dpi=200, bbox_inches='tight', facecolor='#0A0A0A')
    plt.close()
    
    return path

def create_competitor_landscape_chart(data):
    """Create competitor analysis (inspired by your slide 3)"""
    
    properties = data['properties']
    
    # Get unique agents
    agents = {}
    for p in properties:
        agent = p.get('agent', 'Private')[:20]
        agents[agent] = agents.get(agent, 0) + 1
    
    # Top 5 agents
    top_agents = sorted(agents.items(), key=lambda x: x[1], reverse=True)[:5]
    
    fig = plt.figure(figsize=(10, 4), facecolor='#0A0A0A')
    gs = fig.add_gridspec(1, 2, width_ratios=[1.2, 1], wspace=0.3)
    
    # Left: Agent bar chart
    ax1 = fig.add_subplot(gs[0])
    ax1.set_facecolor('#0A0A0A')
    
    agent_names = [a[0] for a in top_agents]
    agent_counts = [a[1] for a in top_agents]
    
    bars = ax1.barh(range(len(agent_names)), agent_counts, color='#D4AF37', 
                    edgecolor='#B8960C', linewidth=1.5)
    
    for i, (bar, count) in enumerate(zip(bars, agent_counts)):
        ax1.text(count + max(agent_counts)*0.02, i, str(count), va='center', ha='left',
                color='#D4AF37', fontsize=10, fontweight='bold')
    
    ax1.set_yticks(range(len(agent_names)))
    ax1.set_yticklabels(agent_names, color='#E8E8E8', fontsize=9)
    ax1.set_xlabel('Listings', color='#E8E8E8', fontsize=9)
    ax1.tick_params(colors='#E8E8E8')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.spines['left'].set_color('#4A4A4A')
    ax1.spines['bottom'].set_color('#4A4A4A')
    ax1.grid(axis='x', alpha=0.2, color='#4A4A4A')
    
    # Right: Market share pie
    ax2 = fig.add_subplot(gs[1])
    ax2.set_facecolor('#0A0A0A')
    
    colors_pie = ['#D4AF37', '#B8960C', '#8B7209', '#6B5507', '#4A4A4A']
    wedges, texts = ax2.pie(agent_counts, startangle=90, colors=colors_pie[:len(agent_counts)])
    
    total = sum(agent_counts)
    for i, (wedge, count) in enumerate(zip(wedges, agent_counts)):
        angle = (wedge.theta2 + wedge.theta1) / 2
        x = 0.7 * np.cos(np.radians(angle))
        y = 0.7 * np.sin(np.radians(angle))
        pct = int(count/total*100)
        ax2.text(x, y, f'{pct}%', ha='center', va='center',
                color='#0A0A0A', fontsize=11, fontweight='bold')
    
    plt.tight_layout()
    
    path = '/tmp/competitor_landscape.png'
    plt.savefig(path, dpi=200, bbox_inches='tight', facecolor='#0A0A0A')
    plt.close()
    
    return path

# ============================================================================
# PDF CONSTRUCTION
# ============================================================================

def create_elite_pdf(data):
    """Create the elite PDF report"""
    
    print(f"\n📄 CREATING ELITE PDF")
    
    doc = SimpleDocTemplate(
        OUTPUT_FILE,
        pagesize=letter,
        leftMargin=0.5*inch,
        rightMargin=0.5*inch,
        topMargin=1.1*inch,
        bottomMargin=0.8*inch
    )
    
    story = []
    styles = getSampleStyleSheet()
    
    # Custom styles
    title_style = ParagraphStyle(
        'EliteTitle',
        parent=styles['Heading1'],
        fontSize=28,
        textColor=COLOR_WHITE,
        spaceAfter=8,
        fontName='Helvetica-Bold',
        leading=32
    )
    
    subtitle_style = ParagraphStyle(
        'EliteSubtitle',
        parent=styles['Normal'],
        fontSize=12,
        textColor=COLOR_GOLD,
        spaceAfter=30,
        fontName='Helvetica-Bold'
    )
    
    section_title_style = ParagraphStyle(
        'SectionTitle',
        parent=styles['Heading2'],
        fontSize=20,
        textColor=COLOR_WHITE,
        spaceBefore=10,
        spaceAfter=15,
        fontName='Helvetica-Bold'
    )
    
    section_subtitle_style = ParagraphStyle(
        'SectionSubtitle',
        parent=styles['Normal'],
        fontSize=10,
        textColor=COLOR_GRAY_LIGHT,
        spaceAfter=20,
        fontName='Helvetica-Oblique'
    )
    
    body_style = ParagraphStyle(
        'EliteBody',
        parent=styles['Normal'],
        fontSize=10,
        textColor=COLOR_GRAY_LIGHT,
        spaceAfter=12,
        leading=14,
        alignment=TA_JUSTIFY
    )
    
    insight_style = ParagraphStyle(
        'Insight',
        parent=styles['Normal'],
        fontSize=11,
        textColor=COLOR_WHITE,
        spaceAfter=10,
        leftIndent=15,
        fontName='Helvetica',
        leading=16,
        borderColor=COLOR_GOLD,
        borderWidth=0,
        borderPadding=10
    )
    
    # ========================================================================
    # PAGE 1: KPI SUMMARY OVERVIEW
    # ========================================================================
    
    metadata = data['metadata']
    metrics = data['metrics']
    
    story.append(Paragraph("KPI SUMMARY", section_title_style))
    story.append(Paragraph("OVERVIEW", title_style))
    story.append(Spacer(1, 0.2*inch))
    
    # Market info box
    info_text = f"""EXECUTIVE KPI OVERVIEW — WEEKLY MARKET PRECISION REPORT<br/>
    <b>{metadata['area'].upper()}, {metadata['city'].upper()}</b> | {metadata['timestamp']}"""
    
    story.append(Paragraph(info_text, subtitle_style))
    story.append(Spacer(1, 0.1*inch))
    
    # KPI chart
    print(f"   → Generating KPI overview chart...")
    kpi_chart = create_kpi_overview_chart(data)
    story.append(Image(kpi_chart, width=7*inch, height=2.8*inch))
    
    story.append(Spacer(1, 0.3*inch))
    
    # Pricing summary table
    pricing_data = [
        ['', 'LOW VALUE', 'HIGH VALUE', 'AVG VALUE'],
        ['Price', f'£{metrics["min_price"]:,}', f'£{metrics["max_price"]:,}', f'£{metrics["avg_price"]:,}'],
        ['Description', 'Represents entry-level\npricing in the market', 
         'Reflects the upper end of\nthe luxury market', 'Indicates overall market\npricing trends']
    ]
    
    pricing_table = Table(pricing_data, colWidths=[1*inch, 2*inch, 2*inch, 2*inch])
    pricing_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), COLOR_BLACK),
        ('BACKGROUND', (1, 1), (-1, 1), COLOR_GRAY_DARK),
        ('BACKGROUND', (1, 2), (-1, 2), COLOR_BLACK),
        ('TEXTCOLOR', (0, 0), (-1, 0), COLOR_GOLD),
        ('TEXTCOLOR', (0, 1), (0, -1), COLOR_GOLD),
        ('TEXTCOLOR', (1, 1), (-1, -1), COLOR_GRAY_LIGHT),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTNAME', (0, 1), (0, -1), 'Helvetica-Bold'),
        ('FONTNAME', (1, 1), (-1, 1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 11),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
        ('TOPPADDING', (0, 0), (-1, -1), 12),
        ('GRID', (0, 0), (-1, -1), 1, COLOR_GRAY_MED),
        ('LINEABOVE', (0, 0), (-1, 0), 2, COLOR_GOLD),
    ]))
    
    story.append(pricing_table)
    
    story.append(PageBreak())
    
    # ========================================================================
    # PAGE 2: PERFORMANCE GRAPH AND MARKET INSIGHTS
    # ========================================================================
    
    story.append(Paragraph("PERFORMANCE GRAPH", section_title_style))
    story.append(Paragraph("AND MARKET INSIGHTS", title_style))
    story.append(Paragraph("Analysis of market performance and strategic market insights", section_subtitle_style))
    
    # Performance charts
    print(f"   → Generating performance insights chart...")
    perf_chart = create_performance_insights_chart(data)
    story.append(Image(perf_chart, width=7*inch, height=3.5*inch))
    
    story.append(Spacer(1, 0.2*inch))
    
    # Key insights box
    story.append(Paragraph("KEY INSIGHTS", section_title_style))
    
    intelligence = data['intelligence']
    trend_text = intelligence.get('trends', 'Market analysis in progress')
    
    story.append(Paragraph(trend_text, body_style))
    
    story.append(PageBreak())
    
    # ========================================================================
    # PAGE 3: COMPETITOR LANDSCAPE ANALYSIS
    # ========================================================================
    
    story.append(Paragraph("COMPETITOR", section_title_style))
    story.append(Paragraph("<font color='#D4AF37'>LANDSCAPE</font>", title_style))
    story.append(Paragraph("ANALYSIS", title_style))
    story.append(Spacer(1, 0.2*inch))
    
    # Competitor chart
    print(f"   → Generating competitor landscape chart...")
    comp_chart = create_competitor_landscape_chart(data)
    story.append(Image(comp_chart, width=7*inch, height=2.8*inch))
    
    story.append(Spacer(1, 0.2*inch))
    
    # Competitor insights
    insight_box_text = """Competitors are actively adjusting strategies to capture market share. 
    Price reductions and new listings are prevalent."""
    
    story.append(Paragraph(insight_box_text, body_style))
    
    story.append(PageBreak())
    
    # ========================================================================
    # PAGE 4: PRICING SUMMARY & KEY INSIGHTS
    # ========================================================================
    
    story.append(Paragraph("PRICING SUMMARY", title_style))
    story.append(Spacer(1, 0.3*inch))
    
    # Reuse pricing table from page 1
    story.append(pricing_table)
    
    story.append(Spacer(1, 0.4*inch))
    
    story.append(Paragraph("KEY INSIGHTS AND", section_title_style))
    story.append(Paragraph("METRICS", title_style))
    story.append(Paragraph("A detailed look at strategic insights and key metrics", section_subtitle_style))
    
    # BLUF
    bluf_text = intelligence.get('bluf', '')
    for line in bluf_text.split('\n'):
        if line.strip():
            story.append(Paragraph(line.strip(), insight_style))
    
    story.append(Spacer(1, 0.2*inch))
    
    # Opportunities
    story.append(Paragraph("STRATEGIC OPPORTUNITIES", section_title_style))
    story.append(Paragraph(intelligence.get('opportunities', ''), body_style))
    
    story.append(Spacer(1, 0.2*inch))
    
    # Risks
    story.append(Paragraph("RISK ASSESSMENT", section_title_style))
    story.append(Paragraph(intelligence.get('risks', ''), body_style))
    
    story.append(PageBreak())
    
    # ========================================================================
    # PAGE 5: TOP PROPERTIES
    # ========================================================================
    
    story.append(Paragraph("TOP OPPORTUNITIES", title_style))
    story.append(Spacer(1, 0.2*inch))
    
    # Top 8 properties table
    top_props = data['properties'][:8]
    
    prop_data = [['ADDRESS', 'PRICE', 'BEDS/BATHS', '£/SQFT', 'SCORE']]
    
    for prop in top_props:
        prop_data.append([
            prop['address'].split(',')[0][:35],
            f"£{prop['price']:,}",
            f"{prop['beds']}/{prop['baths']}",
            f"£{prop['price_per_sqft']:,}",
            f"{prop['deal_score']}/10"
        ])
    
    prop_table = Table(prop_data, colWidths=[2.5*inch, 1.3*inch, 0.9*inch, 0.9*inch, 0.7*inch])
    prop_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), COLOR_BLACK),
        ('BACKGROUND', (0, 1), (-1, -1), COLOR_GRAY_DARK),
        ('TEXTCOLOR', (0, 0), (-1, 0), COLOR_GOLD),
        ('TEXTCOLOR', (0, 1), (-1, -1), COLOR_GRAY_LIGHT),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('ALIGN', (1, 1), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 8),
        ('TOPPADDING', (0, 0), (-1, -1), 8),
        ('GRID', (0, 0), (-1, -1), 0.5, COLOR_GRAY_MED),
        ('LINEABOVE', (0, 0), (-1, 0), 2, COLOR_GOLD),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [COLOR_GRAY_DARK, COLOR_BLACK])
    ]))
    
    story.append(prop_table)
    
    story.append(Spacer(1, 0.3*inch))
    
    # Closing statement
    closing_text = """<b>INSIGHTS SUMMARY</b><br/><br/>
    Key insights highlighted above represent immediate market opportunities. 
    We'll follow up within 24-48 hours to discuss strategic implications for your portfolio and competitive positioning."""
    
    story.append(Paragraph(closing_text, body_style))
    
    # Build PDF
    print(f"   → Building PDF with custom template...")
    doc.build(story, canvasmaker=VoxmillEliteTemplate)
    
    print(f"   ✅ Elite PDF created: {OUTPUT_FILE}")
    return OUTPUT_FILE

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main execution"""
    
    print("\n" + "="*70)
    print("VOXMILL ELITE PDF GENERATOR")
    print("="*70)
    
    try:
        if not os.path.exists(INPUT_FILE):
            raise Exception(f"Intelligence data not found: {INPUT_FILE}")
        
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        pdf_file = create_elite_pdf(data)
        
        print(f"\n✅ PDF generation complete")
        return pdf_file
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
