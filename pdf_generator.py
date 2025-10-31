"""
VOXMILL CINEMA-GRADE PDF GENERATOR
==================================
LexCura-inspired luxury PDF design
Rich black background, strategic gold accents, perfect white text
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
import numpy as np

INPUT_FILE = "/tmp/voxmill_analysis.json"
OUTPUT_FILE = "/tmp/Voxmill_Elite_Intelligence.pdf"

# LexCura-level Cinema Palette
COLOR_RICH_BLACK = colors.HexColor('#0A0A0A')
COLOR_PURE_WHITE = colors.HexColor('#FFFFFF')
COLOR_GOLD = colors.HexColor('#CBA135')
COLOR_GOLD_LIGHT = colors.HexColor('#D4B55A')
COLOR_GOLD_DARK = colors.HexColor('#8B6F2E')
COLOR_GREY_LIGHT = colors.HexColor('#E8E8E8')
COLOR_GREY_MED = colors.HexColor('#B8B8B8')
COLOR_GREY_DARK = colors.HexColor('#666666')
COLOR_ACCENT_BOX = colors.HexColor('#111111')

# ============================================================================
# CINEMA-GRADE PAGE TEMPLATE WITH BLACK BACKGROUND
# ============================================================================

class VoxmillCinemaTemplate(canvas.Canvas):
    """LexCura-level cinema template with rich black background"""
    
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
            self.draw_cinema_elements(i + 1, page_count)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)
        
    def draw_cinema_elements(self, page_num, page_count):
        """Draw LexCura-level cinema page elements"""
        
        # CRITICAL: Fill entire page with rich black background
        self.setFillColor(COLOR_RICH_BLACK)
        self.rect(0, 0, 8.5*inch, 11*inch, fill=1, stroke=0)
        
        # Top gold accent line (LexCura style)
        self.setStrokeColor(COLOR_GOLD)
        self.setLineWidth(3)
        self.line(40, 10.65*inch, 8.5*inch - 40, 10.65*inch)
        
        # Voxmill diamond logo (center top)
        center_x = 4.25*inch
        diamond_size = 12
        
        # Draw diamond
        self.setFillColor(COLOR_GOLD)
        diamond_points = [
            (center_x, 10.85*inch + diamond_size),
            (center_x + diamond_size, 10.85*inch),
            (center_x, 10.85*inch - diamond_size),
            (center_x - diamond_size, 10.85*inch)
        ]
        path = self.beginPath()
        path.moveTo(*diamond_points[0])
        for point in diamond_points[1:]:
            path.lineTo(*point)
        path.close()
        self.drawPath(path, fill=1)
        
        # V in diamond
        self.setFont('Times-Bold', 14)
        self.setFillColor(COLOR_RICH_BLACK)
        self.drawCentredString(center_x, 10.82*inch, "V")
        
        # VOXMILL brand
        self.setFont('Helvetica-Bold', 16)
        self.setFillColor(COLOR_PURE_WHITE)
        self.drawCentredString(center_x, 10.55*inch, "VOXMILL")
        
        self.setFont('Helvetica', 9)
        self.setFillColor(COLOR_GOLD)
        self.drawCentredString(center_x, 10.4*inch, "MARKET INTELLIGENCE")
        
        # Footer elements (LexCura style)
        self.setFont('Helvetica', 7)
        self.setFillColor(COLOR_GREY_DARK)
        
        # Left: Classification
        self.drawString(40, 0.4*inch, "CONFIDENTIAL MARKET INTELLIGENCE")
        
        # Center: Copyright
        self.drawCentredString(4.25*inch, 0.4*inch, 
                              f"© {datetime.now().year} Voxmill Automations • All rights reserved")
        
        # Right: Page number
        self.setFillColor(COLOR_GOLD)
        self.drawRightString(8.5*inch - 40, 0.4*inch, f"PAGE {page_num} OF {page_count}")
        
        # Bottom gold line
        self.setStrokeColor(COLOR_GOLD)
        self.setLineWidth(1)
        self.line(40, 0.55*inch, 8.5*inch - 40, 0.55*inch)

# ============================================================================
# CINEMA-GRADE CHART GENERATION (BLACK BACKGROUNDS)
# ============================================================================

def create_cinema_kpi_chart(data):
    """Cinema-grade KPI chart with rich black background"""
    
    metrics = data['metrics']
    properties = data['properties']
    
    # Count deal tiers
    exceptional = sum(1 for p in properties if p.get('deal_score', 0) >= 9)
    hot = sum(1 for p in properties if 7 <= p.get('deal_score', 0) < 9)
    strong = sum(1 for p in properties if 5 <= p.get('deal_score', 0) < 7)
    
    # Set up figure with rich black background
    plt.style.use('dark_background')
    fig = plt.figure(figsize=(10, 4), facecolor='#0A0A0A')
    gs = fig.add_gridspec(1, 2, width_ratios=[1, 2], wspace=0.4)
    
    # Left: Property count
    ax1 = fig.add_subplot(gs[0])
    ax1.set_facecolor('#0A0A0A')
    
    bars1 = ax1.bar([0], [metrics['total_properties']], width=0.5, 
                    color='#CBA135', edgecolor='#D4B55A', linewidth=2)
    
    # Add glow effect
    ax1.bar([0], [metrics['total_properties']], width=0.6, 
            color='#CBA135', alpha=0.3, zorder=0)
    
    ax1.set_xlim(-0.6, 0.6)
    ax1.set_ylim(0, metrics['total_properties'] * 1.2)
    ax1.set_xticks([0])
    ax1.set_xticklabels(['ANALYZED'], color='#FFFFFF', fontsize=11, fontweight='600')
    ax1.set_ylabel('PROPERTIES', color='#CBA135', fontsize=10, fontweight='600')
    ax1.tick_params(colors='#FFFFFF')
    
    # Style spines
    for spine in ax1.spines.values():
        spine.set_color('#333333')
        spine.set_linewidth(0.5)
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    
    ax1.grid(axis='y', alpha=0.2, color='#333333', linestyle='-', linewidth=0.5)
    
    # Right: Deal tiers
    ax2 = fig.add_subplot(gs[1])
    ax2.set_facecolor('#0A0A0A')
    
    categories = ['EXCEPTIONAL\nDEALS', 'HOT\nDEALS', 'STRONG\nVALUE']
    values = [exceptional, hot, strong]
    
    bars2 = ax2.barh(categories, values, color='#CBA135', 
                     edgecolor='#D4B55A', linewidth=1.5, alpha=0.9)
    
    # Glow effect
    ax2.barh(categories, values, color='#CBA135', alpha=0.2, zorder=0)
    
    # Value labels
    max_val = max(values) if values else 1
    for i, (bar, val) in enumerate(zip(bars2, values)):
        ax2.text(val + max_val*0.02, i, str(val), va='center', ha='left',
                color='#CBA135', fontsize=12, fontweight='bold')
    
    ax2.set_xlabel('COUNT', color='#CBA135', fontsize=10, fontweight='600')
    ax2.tick_params(colors='#FFFFFF')
    
    for spine in ax2.spines.values():
        spine.set_color('#333333')
        spine.set_linewidth(0.5)
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    
    ax2.grid(axis='x', alpha=0.2, color='#333333', linestyle='-', linewidth=0.5)
    
    plt.tight_layout()
    
    path = '/tmp/cinema_kpi.png'
    plt.savefig(path, dpi=250, bbox_inches='tight', facecolor='#0A0A0A', 
                edgecolor='none', transparent=False)
    plt.close()
    
    return path

def create_cinema_performance_chart(data):
    """Cinema-grade performance dashboard"""
    
    properties = data['properties'][:10]
    
    plt.style.use('dark_background')
    fig = plt.figure(figsize=(10, 6), facecolor='#0A0A0A')
    gs = fig.add_gridspec(2, 2, hspace=0.4, wspace=0.4)
    
    # Top left: Price bars
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.set_facecolor('#0A0A0A')
    
    prices = [p['price']/1000000 for p in properties[:6]]
    addresses = [p['address'].split(',')[0][:16] for p in properties[:6]]
    
    y_pos = range(len(prices))
    bars = ax1.barh(y_pos, prices, color='#CBA135', edgecolor='#D4B55A', 
                    linewidth=1, alpha=0.9)
    
    # Glow
    ax1.barh(y_pos, prices, color='#CBA135', alpha=0.2, zorder=0)
    
    ax1.set_yticks(y_pos)
    ax1.set_yticklabels(addresses, color='#FFFFFF', fontsize=9)
    ax1.set_xlabel('PRICE (£M)', color='#CBA135', fontsize=9)
    ax1.tick_params(colors='#FFFFFF')
    
    for spine in ax1.spines.values():
        spine.set_color('#333333')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.grid(axis='x', alpha=0.2, color='#333333')
    
    # Top right: Trend line
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.set_facecolor('#0A0A0A')
    
    x = list(range(len(properties)))
    y = [p['price_per_sqft'] for p in properties]
    
    ax2.plot(x, y, color='#CBA135', linewidth=3, marker='o', markersize=7, 
             markerfacecolor='#CBA135', markeredgecolor='#D4B55A', 
             markeredgewidth=1.5, alpha=0.9)
    
    # Glow fill
    ax2.fill_between(x, y, alpha=0.25, color='#CBA135')
    
    ax2.set_xlabel('PROPERTY RANK', color='#CBA135', fontsize=9)
    ax2.set_ylabel('£ / SQFT', color='#CBA135', fontsize=9)
    ax2.tick_params(colors='#FFFFFF')
    
    for spine in ax2.spines.values():
        spine.set_color('#333333')
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.grid(alpha=0.2, color='#333333')
    
    # Bottom left: Property types pie
    ax3 = fig.add_subplot(gs[1, 0])
    ax3.set_facecolor('#0A0A0A')
    
    types = {}
    for p in properties:
        ptype = p['property_type']
        types[ptype] = types.get(ptype, 0) + 1
    
    gold_palette = ['#CBA135', '#D4B55A', '#8B6F2E', '#6B5522']
    wedges, texts = ax3.pie(types.values(), startangle=90, 
                            colors=gold_palette[:len(types)])
    
    # Percentage labels
    total = sum(types.values())
    for wedge, count in zip(wedges, types.values()):
        angle = (wedge.theta2 + wedge.theta1) / 2
        x = 0.65 * np.cos(np.radians(angle))
        y = 0.65 * np.sin(np.radians(angle))
        pct = int(count/total*100)
        ax3.text(x, y, f'{pct}%', ha='center', va='center',
                color='#0A0A0A', fontsize=12, fontweight='bold')
    
    ax3.legend(types.keys(), loc='lower left', fontsize=8, 
              facecolor='#111111', edgecolor='#333333', 
              labelcolor='#FFFFFF', framealpha=0.9)
    
    # Bottom right: Deal scores
    ax4 = fig.add_subplot(gs[1, 1])
    ax4.set_facecolor('#0A0A0A')
    
    scores = [p['deal_score'] for p in properties]
    colors_bars = ['#CBA135' if s >= 8 else '#D4B55A' if s >= 7 else '#8B6F2E' for s in scores]
    
    bars = ax4.bar(range(len(scores)), scores, color=colors_bars, 
                   edgecolor='#333333', linewidth=1, alpha=0.9)
    
    # Glow
    ax4.bar(range(len(scores)), scores, color='#CBA135', alpha=0.15, zorder=0)
    
    # Reference lines
    ax4.axhline(y=8, color='#CBA135', linestyle='--', linewidth=1, alpha=0.5)
    ax4.axhline(y=7, color='#D4B55A', linestyle='--', linewidth=1, alpha=0.5)
    
    ax4.set_xlabel('PROPERTY', color='#CBA135', fontsize=9)
    ax4.set_ylabel('DEAL SCORE', color='#CBA135', fontsize=9)
    ax4.set_ylim(0, 10)
    ax4.tick_params(colors='#FFFFFF')
    
    for spine in ax4.spines.values():
        spine.set_color('#333333')
    ax4.spines['top'].set_visible(False)
    ax4.spines['right'].set_visible(False)
    ax4.grid(axis='y', alpha=0.2, color='#333333')
    
    plt.tight_layout()
    
    path = '/tmp/cinema_performance.png'
    plt.savefig(path, dpi=250, bbox_inches='tight', facecolor='#0A0A0A',
                edgecolor='none', transparent=False)
    plt.close()
    
    return path

def create_cinema_competitor_chart(data):
    """Cinema-grade competitor analysis"""
    
    properties = data['properties']
    
    agents = {}
    for p in properties:
        agent = p.get('agent', 'Private')[:20]
        agents[agent] = agents.get(agent, 0) + 1
    
    top_agents = sorted(agents.items(), key=lambda x: x[1], reverse=True)[:5]
    
    plt.style.use('dark_background')
    fig = plt.figure(figsize=(10, 4), facecolor='#0A0A0A')
    gs = fig.add_gridspec(1, 2, width_ratios=[1.2, 1], wspace=0.4)
    
    # Left: Agent bars
    ax1 = fig.add_subplot(gs[0])
    ax1.set_facecolor('#0A0A0A')
    
    agent_names = [a[0] for a in top_agents]
    agent_counts = [a[1] for a in top_agents]
    
    y_pos = range(len(agent_names))
    bars = ax1.barh(y_pos, agent_counts, color='#CBA135', 
                    edgecolor='#D4B55A', linewidth=1.5, alpha=0.9)
    
    # Glow
    ax1.barh(y_pos, agent_counts, color='#CBA135', alpha=0.2, zorder=0)
    
    max_val = max(agent_counts) if agent_counts else 1
    for i, count in enumerate(agent_counts):
        ax1.text(count + max_val*0.02, i, str(count), va='center', ha='left',
                color='#CBA135', fontsize=11, fontweight='bold')
    
    ax1.set_yticks(y_pos)
    ax1.set_yticklabels(agent_names, color='#FFFFFF', fontsize=9)
    ax1.set_xlabel('LISTINGS', color='#CBA135', fontsize=9)
    ax1.tick_params(colors='#FFFFFF')
    
    for spine in ax1.spines.values():
        spine.set_color('#333333')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.grid(axis='x', alpha=0.2, color='#333333')
    
    # Right: Market share pie
    ax2 = fig.add_subplot(gs[1])
    ax2.set_facecolor('#0A0A0A')
    
    gold_shades = ['#CBA135', '#D4B55A', '#8B6F2E', '#6B5522', '#9A7A3E']
    wedges, texts = ax2.pie(agent_counts, startangle=90, 
                            colors=gold_shades[:len(agent_counts)])
    
    total = sum(agent_counts)
    for wedge, count in zip(wedges, agent_counts):
        angle = (wedge.theta2 + wedge.theta1) / 2
        x = 0.65 * np.cos(np.radians(angle))
        y = 0.65 * np.sin(np.radians(angle))
        pct = int(count/total*100)
        ax2.text(x, y, f'{pct}%', ha='center', va='center',
                color='#0A0A0A', fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    
    path = '/tmp/cinema_competitors.png'
    plt.savefig(path, dpi=250, bbox_inches='tight', facecolor='#0A0A0A',
                edgecolor='none', transparent=False)
    plt.close()
    
    return path

# ============================================================================
# CINEMA-GRADE PDF CONSTRUCTION
# ============================================================================

def create_cinema_pdf(data):
    """Create LexCura-level cinema PDF"""
    
    print(f"\n📄 CREATING CINEMA-GRADE PDF")
    
    doc = SimpleDocTemplate(
        OUTPUT_FILE,
        pagesize=letter,
        leftMargin=40,
        rightMargin=40,
        topMargin=1.1*inch,
        bottomMargin=0.8*inch
    )
    
    story = []
    styles = getSampleStyleSheet()
    
    # Cinema-grade styles
    title_cinema = ParagraphStyle(
        'CinemaTitle',
        parent=styles['Heading1'],
        fontSize=32,
        textColor=COLOR_PURE_WHITE,
        spaceAfter=8,
        fontName='Helvetica-Bold',
        leading=36,
        alignment=TA_CENTER
    )
    
    subtitle_cinema = ParagraphStyle(
        'CinemaSubtitle',
        parent=styles['Normal'],
        fontSize=11,
        textColor=COLOR_GOLD,
        spaceAfter=25,
        fontName='Helvetica',
        leading=16,
        alignment=TA_CENTER
    )
    
    section_cinema = ParagraphStyle(
        'CinemaSection',
        parent=styles['Heading2'],
        fontSize=20,
        textColor=COLOR_PURE_WHITE,
        spaceBefore=8,
        spaceAfter=12,
        fontName='Helvetica-Bold',
        leading=24,
        alignment=TA_CENTER
    )
    
    body_cinema = ParagraphStyle(
        'CinemaBody',
        parent=styles['Normal'],
        fontSize=11,
        textColor=COLOR_GREY_LIGHT,
        spaceAfter=14,
        leading=17,
        alignment=TA_JUSTIFY
    )
    
    highlight_cinema = ParagraphStyle(
        'CinemaHighlight',
        parent=styles['Normal'],
        fontSize=10,
        textColor=COLOR_PURE_WHITE,
        spaceAfter=10,
        leftIndent=20,
        fontName='Helvetica',
        leading=15
    )
    
    # Extract data
    metadata = data['metadata']
    metrics = data['metrics']
    intelligence = data['intelligence']
    
    # ========================================================================
    # PAGE 1: TITLE PAGE
    # ========================================================================
    
    story.append(Spacer(1, 0.5*inch))
    story.append(Paragraph("MARKET INTELLIGENCE", section_cinema))
    story.append(Paragraph("SNAPSHOT", title_cinema))
    story.append(Spacer(1, 0.3*inch))
    
    context = f"""WEEKLY PRECISION REPORT • CONFIDENTIAL ANALYSIS<br/>
    <b>{metadata['area'].upper()}, {metadata['city'].upper()}</b><br/>
    {datetime.now().strftime('%B %d, %Y')}"""
    
    story.append(Paragraph(context, subtitle_cinema))
    story.append(Spacer(1, 0.4*inch))
    
    # KPI overview
    print(f"   → Generating cinema KPI chart...")
    kpi_chart = create_cinema_kpi_chart(data)
    story.append(Image(kpi_chart, width=7*inch, height=2.8*inch))
    
    story.append(Spacer(1, 0.3*inch))
    
    # Key metrics table (cinema style)
    metrics_data = [
        ['METRIC', 'LOW VALUE', 'HIGH VALUE', 'AVERAGE'],
        ['PRICE', f'£{metrics["min_price"]:,}', f'£{metrics["max_price"]:,}', f'£{metrics["avg_price"]:,.0f}'],
        ['ANALYSIS', 'Entry-level pricing threshold', 'Luxury market ceiling', 'Central market trend']
    ]
    
    metrics_table = Table(metrics_data, colWidths=[1.1*inch, 2*inch, 2*inch, 2*inch])
    metrics_table.setStyle(TableStyle([
        # Header row (gold background)
        ('BACKGROUND', (0, 0), (-1, 0), COLOR_GOLD),
        ('TEXTCOLOR', (0, 0), (-1, 0), COLOR_RICH_BLACK),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        
        # Data rows (dark backgrounds)
        ('BACKGROUND', (0, 1), (-1, 1), COLOR_ACCENT_BOX),
        ('BACKGROUND', (0, 2), (-1, 2), COLOR_RICH_BLACK),
        ('TEXTCOLOR', (1, 1), (-1, 1), COLOR_GOLD),
        ('TEXTCOLOR', (1, 2), (-1, 2), COLOR_GREY_LIGHT),
        ('FONTNAME', (0, 1), (0, 2), 'Helvetica-Bold'),
        ('FONTNAME', (1, 1), (-1, 1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        ('TEXTCOLOR', (0, 1), (0, 2), COLOR_GOLD),
        
        # Alignment and padding
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 12),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
        
        # Borders
        ('GRID', (0, 0), (-1, -1), 0.5, COLOR_GREY_DARK),
        ('LINEABOVE', (0, 0), (-1, 0), 2, COLOR_GOLD),
    ]))
    
    story.append(metrics_table)
    story.append(PageBreak())
    
    # ========================================================================
    # PAGE 2: PERFORMANCE DASHBOARD
    # ========================================================================
    
    story.append(Paragraph("PERFORMANCE GRAPH", title_cinema))
    story.append(Paragraph("STRATEGIC MARKET ANALYSIS", subtitle_cinema))
    
    print(f"   → Generating cinema performance chart...")
    perf_chart = create_cinema_performance_chart(data)
    story.append(Image(perf_chart, width=7*inch, height=4.2*inch))
    
    story.append(Spacer(1, 0.2*inch))
    
    # Key insights
    story.append(Paragraph("KEY INSIGHTS", section_cinema))
    
    summary = intelligence.get('executive_summary', 'Market analysis in progress.')
    story.append(Paragraph(summary, body_cinema))
    
    if 'strategic_insights' in intelligence:
        for insight in intelligence['strategic_insights'][:3]:
            story.append(Paragraph(f"• {insight}", highlight_cinema))
    
    story.append(PageBreak())
    
    # ========================================================================
    # PAGE 3: COMPETITOR LANDSCAPE
    # ========================================================================
    
    story.append(Paragraph("COMPETITOR", title_cinema))
    story.append(Paragraph("<font color='#CBA135'>LANDSCAPE</font> ANALYSIS", title_cinema))
    story.append(Spacer(1, 0.2*inch))
    
    print(f"   → Generating cinema competitor chart...")
    comp_chart = create_cinema_competitor_chart(data)
    story.append(Image(comp_chart, width=7*inch, height=2.8*inch))
    
    story.append(Spacer(1, 0.25*inch))
    
    comp_text = """Market participants demonstrate active positioning adjustments across established agencies. 
    Price recalibrations and new inventory introductions reflect competitive dynamics within 
    the luxury real estate ecosystem."""
    story.append(Paragraph(comp_text, body_cinema))
    
    story.append(PageBreak())
    
    # ========================================================================
    # PAGE 4: STRATEGIC INTELLIGENCE
    # ========================================================================
    
    story.append(Paragraph("STRATEGIC", title_cinema))
    story.append(Paragraph("<font color='#CBA135'>INTELLIGENCE</font>", title_cinema))
    story.append(Spacer(1, 0.25*inch))
    
    # Executive summary
    story.append(Paragraph("EXECUTIVE SUMMARY", section_cinema))
    summary = intelligence.get('executive_summary', 'Intelligence synthesis in progress.')
    story.append(Paragraph(summary, body_cinema))
    
    story.append(Spacer(1, 0.2*inch))
    
    # Tactical opportunities
    if 'tactical_opportunities' in intelligence:
        story.append(Paragraph("TACTICAL OPPORTUNITIES", section_cinema))
        tact = intelligence['tactical_opportunities']
        if isinstance(tact, dict):
            for key, val in tact.items():
                story.append(Paragraph(f"<b>{key.upper()}:</b> {val}", body_cinema))
    
    story.append(Spacer(1, 0.2*inch))
    
    # Risk assessment
    story.append(Paragraph("RISK ASSESSMENT", section_cinema))
    risk = intelligence.get('risk_assessment', 'Standard market risk parameters apply.')
    story.append(Paragraph(risk, body_cinema))
    
    story.append(Spacer(1, 0.2*inch))
    
    # Market sentiment
    sentiment = intelligence.get('market_sentiment', 'Neutral')
    confidence = intelligence.get('confidence_level', 'Medium')
    sentiment_text = f"<b>MARKET SENTIMENT:</b> {sentiment}  |  <b>CONFIDENCE LEVEL:</b> {confidence}"
    story.append(Paragraph(sentiment_text, subtitle_cinema))
    
    story.append(PageBreak())
    
    # ========================================================================
    # PAGE 5: TOP OPPORTUNITIES
    # ========================================================================
    
    story.append(Paragraph("TOP OPPORTUNITIES", title_cinema))
    story.append(Spacer(1, 0.2*inch))
    
    # Opportunities table (cinema style)
    top_props = data['top_opportunities'][:8]
    
    table_data = [['ADDRESS', 'PRICE', 'BEDS/BATHS', '£/SQFT', 'SCORE']]
    
    for prop in top_props:
        table_data.append([
            prop['address'].split(',')[0][:35],
            f"£{prop['price']:,}",
            f"{prop['beds']}/{prop['baths']}",
            f"£{prop['price_per_sqft']:,.0f}",
            f"{prop['deal_score']}/10"
        ])
    
    opp_table = Table(table_data, colWidths=[2.6*inch, 1.3*inch, 0.9*inch, 0.9*inch, 0.7*inch])
    opp_table.setStyle(TableStyle([
        # Header (gold)
        ('BACKGROUND', (0, 0), (-1, 0), COLOR_GOLD),
        ('TEXTCOLOR', (0, 0), (-1, 0), COLOR_RICH_BLACK),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        
        # Data rows (alternating dark backgrounds)
        ('BACKGROUND', (0, 1), (-1, -1), COLOR_ACCENT_BOX),
        ('ROWBACKGROUNDS', (0, 1), (-1, -1), [COLOR_ACCENT_BOX, COLOR_RICH_BLACK]),
        ('TEXTCOLOR', (0, 1), (-1, -1), COLOR_PURE_WHITE),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        
        # Alignment
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('ALIGN', (1, 1), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        
        # Padding
        ('TOPPADDING', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        
        # Borders
        ('GRID', (0, 0), (-1, -1), 0.5, COLOR_GREY_DARK),
        ('LINEABOVE', (0, 0), (-1, 0), 2, COLOR_GOLD),
    ]))
    
    story.append(opp_table)
    
    story.append(Spacer(1, 0.3*inch))
    
    # Closing statement
    closing = """<b>INTELLIGENCE SUMMARY</b><br/><br/>
    Analysis represents immediate market opportunities derived from proprietary intelligence systems. 
    Designed to support executive-level strategic positioning and competitive advantage 
    in luxury real estate markets."""
    
    story.append(Paragraph(closing, body_cinema))
    
    # Build PDF with cinema template
    print(f"   → Building cinema-grade PDF...")
    doc.build(story, canvasmaker=VoxmillCinemaTemplate)
    
    print(f"   ✅ Cinema PDF created: {OUTPUT_FILE}")
    return OUTPUT_FILE

# ============================================================================
# MAIN
# ============================================================================

def main():
    """Main execution"""
    
    print("\n" + "="*70)
    print("VOXMILL CINEMA-GRADE PDF GENERATOR")
    print("="*70)
    
    try:
        if not os.path.exists(INPUT_FILE):
            raise Exception(f"Intelligence data not found: {INPUT_FILE}")
        
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        pdf_file = create_cinema_pdf(data)
        
        print(f"\n✅ Cinema-grade PDF generation complete")
        return pdf_file
        
    except Exception as e:
        print(f"\n❌ CRITICAL ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
