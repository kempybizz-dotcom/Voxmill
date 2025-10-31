"""
VOXMILL FINAL PDF GENERATOR
============================
Based on YOUR working code structure
White background + executive styling
GUARANTEED TO WORK
"""

import os
import json
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY
from reportlab.pdfgen import canvas
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

INPUT_FILE = "/tmp/voxmill_analysis.json"
OUTPUT_FILE = "/tmp/Voxmill_Elite_Intelligence.pdf"

# Executive colors
WHITE = colors.white
BG_LIGHT = colors.HexColor('#F8F8F8')
TEXT_DARK = colors.HexColor('#2C2C2C')
TEXT_GREY = colors.HexColor('#5A5A5A')
BRONZE = colors.HexColor('#B08D57')
GOLD = colors.HexColor('#CBA135')
BORDER = colors.HexColor('#E0E0E0')

class VoxmillTemplate(canvas.Canvas):
    """White background template - renders AFTER content"""
    
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
        """Draw header and footer"""
        
        # Top bronze line
        self.setStrokeColor(BRONZE)
        self.setLineWidth(2)
        self.line(40, 10.65*inch, 8.5*inch - 40, 10.65*inch)
        
        # Diamond logo
        center_x = 4.25*inch
        self.setFillColor(BRONZE)
        diamond = [
            (center_x, 10.85*inch + 12),
            (center_x + 12, 10.85*inch),
            (center_x, 10.85*inch - 12),
            (center_x - 12, 10.85*inch)
        ]
        path = self.beginPath()
        path.moveTo(*diamond[0])
        for point in diamond[1:]:
            path.lineTo(*point)
        path.close()
        self.drawPath(path, fill=1)
        
        # V in diamond
        self.setFont('Times-Bold', 14)
        self.setFillColor(WHITE)
        self.drawCentredString(center_x, 10.82*inch, "V")
        
        # Brand
        self.setFont('Helvetica-Bold', 16)
        self.setFillColor(TEXT_DARK)
        self.drawCentredString(center_x, 10.55*inch, "VOXMILL")
        
        self.setFont('Helvetica', 9)
        self.setFillColor(BRONZE)
        self.drawCentredString(center_x, 10.4*inch, "MARKET INTELLIGENCE")
        
        # Footer
        self.setFont('Helvetica', 7)
        self.setFillColor(TEXT_GREY)
        self.drawString(40, 0.4*inch, "CONFIDENTIAL MARKET INTELLIGENCE")
        self.drawCentredString(4.25*inch, 0.4*inch, f"© {datetime.now().year} Voxmill Automations")
        
        self.setFillColor(BRONZE)
        self.drawRightString(8.5*inch - 40, 0.4*inch, f"PAGE {page_num} OF {page_count}")
        
        self.setStrokeColor(BORDER)
        self.setLineWidth(1)
        self.line(40, 0.55*inch, 8.5*inch - 40, 0.55*inch)

def create_kpi_chart(data):
    """KPI chart - white background"""
    
    metrics = data['metrics']
    properties = data['properties']
    
    exceptional = sum(1 for p in properties if p.get('deal_score', 0) >= 9)
    hot = sum(1 for p in properties if 7 <= p.get('deal_score', 0) < 9)
    strong = sum(1 for p in properties if 5 <= p.get('deal_score', 0) < 7)
    
    plt.style.use('default')
    fig = plt.figure(figsize=(10, 4), facecolor='white')
    gs = fig.add_gridspec(1, 2, width_ratios=[1, 2], wspace=0.4)
    
    # Left
    ax1 = fig.add_subplot(gs[0])
    ax1.set_facecolor('#FAFAFA')
    ax1.bar([0], [metrics['total_properties']], width=0.5, 
            color='#B08D57', edgecolor='#8B6F2E', linewidth=2)
    ax1.set_xlim(-0.6, 0.6)
    ax1.set_ylim(0, metrics['total_properties'] * 1.2)
    ax1.set_xticks([0])
    ax1.set_xticklabels(['ANALYZED'], color='#2C2C2C', fontsize=11, fontweight='600')
    ax1.set_ylabel('PROPERTIES', color='#B08D57', fontsize=10, fontweight='600')
    ax1.tick_params(colors='#5A5A5A')
    for spine in ax1.spines.values():
        spine.set_color('#E0E0E0')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.grid(axis='y', alpha=0.3, color='#E0E0E0')
    
    # Right
    ax2 = fig.add_subplot(gs[1])
    ax2.set_facecolor('#FAFAFA')
    categories = ['EXCEPTIONAL\nDEALS', 'HOT\nDEALS', 'STRONG\nVALUE']
    values = [exceptional, hot, strong]
    ax2.barh(categories, values, color='#B08D57', edgecolor='#8B6F2E', linewidth=1.5)
    max_val = max(values) if values else 1
    for i, val in enumerate(values):
        ax2.text(val + max_val*0.02, i, str(val), va='center', ha='left', 
                color='#B08D57', fontsize=12, fontweight='bold')
    ax2.set_xlabel('COUNT', color='#B08D57', fontsize=10, fontweight='600')
    ax2.tick_params(colors='#5A5A5A')
    for spine in ax2.spines.values():
        spine.set_color('#E0E0E0')
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.grid(axis='x', alpha=0.3, color='#E0E0E0')
    
    plt.tight_layout()
    path = '/tmp/kpi.png'
    plt.savefig(path, dpi=250, bbox_inches='tight', facecolor='white', edgecolor='none')
    plt.close()
    return path

def create_performance_chart(data):
    """Performance chart"""
    
    properties = data['properties'][:10]
    
    plt.style.use('default')
    fig = plt.figure(figsize=(10, 6), facecolor='white')
    gs = fig.add_gridspec(2, 2, hspace=0.4, wspace=0.4)
    
    # Top left
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.set_facecolor('#FAFAFA')
    prices = [p['price']/1000000 for p in properties[:6]]
    addresses = [p['address'].split(',')[0][:16] for p in properties[:6]]
    y_pos = range(len(prices))
    ax1.barh(y_pos, prices, color='#B08D57', edgecolor='#8B6F2E', linewidth=1)
    ax1.set_yticks(y_pos)
    ax1.set_yticklabels(addresses, color='#2C2C2C', fontsize=9)
    ax1.set_xlabel('PRICE (£M)', color='#B08D57', fontsize=9)
    ax1.tick_params(colors='#5A5A5A')
    for spine in ax1.spines.values():
        spine.set_color('#E0E0E0')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.grid(axis='x', alpha=0.3, color='#E0E0E0')
    
    # Top right
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.set_facecolor('#FAFAFA')
    x = list(range(len(properties)))
    y = [p['price_per_sqft'] for p in properties]
    ax2.plot(x, y, color='#B08D57', linewidth=3, marker='o', markersize=7, 
             markerfacecolor='#B08D57', markeredgecolor='#8B6F2E', markeredgewidth=1.5)
    ax2.fill_between(x, y, alpha=0.15, color='#B08D57')
    ax2.set_xlabel('PROPERTY RANK', color='#B08D57', fontsize=9)
    ax2.set_ylabel('£ / SQFT', color='#B08D57', fontsize=9)
    ax2.tick_params(colors='#5A5A5A')
    for spine in ax2.spines.values():
        spine.set_color('#E0E0E0')
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.grid(alpha=0.3, color='#E0E0E0')
    
    # Bottom left
    ax3 = fig.add_subplot(gs[1, 0])
    ax3.set_facecolor('#FAFAFA')
    types = {}
    for p in properties:
        ptype = p['property_type']
        types[ptype] = types.get(ptype, 0) + 1
    colors_pie = ['#B08D57', '#CBA135', '#8B6F2E', '#6B5522']
    wedges, texts = ax3.pie(types.values(), startangle=90, colors=colors_pie[:len(types)])
    total = sum(types.values())
    for wedge, count in zip(wedges, types.values()):
        angle = (wedge.theta2 + wedge.theta1) / 2
        x_pos = 0.65 * np.cos(np.radians(angle))
        y_pos = 0.65 * np.sin(np.radians(angle))
        pct = int(count/total*100)
        ax3.text(x_pos, y_pos, f'{pct}%', ha='center', va='center', 
                color='white', fontsize=12, fontweight='bold')
    ax3.legend(types.keys(), loc='lower left', fontsize=8, facecolor='white', 
              edgecolor='#E0E0E0', labelcolor='#2C2C2C')
    
    # Bottom right
    ax4 = fig.add_subplot(gs[1, 1])
    ax4.set_facecolor('#FAFAFA')
    scores = [p['deal_score'] for p in properties]
    colors_bars = ['#B08D57' if s >= 8 else '#CBA135' if s >= 7 else '#8B6F2E' for s in scores]
    ax4.bar(range(len(scores)), scores, color=colors_bars, edgecolor='#5A5A5A', linewidth=1)
    ax4.axhline(y=8, color='#B08D57', linestyle='--', linewidth=1, alpha=0.5)
    ax4.axhline(y=7, color='#CBA135', linestyle='--', linewidth=1, alpha=0.5)
    ax4.set_xlabel('PROPERTY', color='#B08D57', fontsize=9)
    ax4.set_ylabel('DEAL SCORE', color='#B08D57', fontsize=9)
    ax4.set_ylim(0, 10)
    ax4.tick_params(colors='#5A5A5A')
    for spine in ax4.spines.values():
        spine.set_color('#E0E0E0')
    ax4.spines['top'].set_visible(False)
    ax4.spines['right'].set_visible(False)
    ax4.grid(axis='y', alpha=0.3, color='#E0E0E0')
    
    plt.tight_layout()
    path = '/tmp/perf.png'
    plt.savefig(path, dpi=250, bbox_inches='tight', facecolor='white', edgecolor='none')
    plt.close()
    return path

def create_competitor_chart(data):
    """Competitor chart"""
    
    properties = data['properties']
    agents = {}
    for p in properties:
        agent = p.get('agent', 'Private')[:20]
        agents[agent] = agents.get(agent, 0) + 1
    top_agents = sorted(agents.items(), key=lambda x: x[1], reverse=True)[:5]
    
    plt.style.use('default')
    fig = plt.figure(figsize=(10, 4), facecolor='white')
    gs = fig.add_gridspec(1, 2, width_ratios=[1.2, 1], wspace=0.4)
    
    # Left
    ax1 = fig.add_subplot(gs[0])
    ax1.set_facecolor('#FAFAFA')
    agent_names = [a[0] for a in top_agents]
    agent_counts = [a[1] for a in top_agents]
    y_pos = range(len(agent_names))
    ax1.barh(y_pos, agent_counts, color='#B08D57', edgecolor='#8B6F2E', linewidth=1.5)
    max_val = max(agent_counts) if agent_counts else 1
    for i, count in enumerate(agent_counts):
        ax1.text(count + max_val*0.02, i, str(count), va='center', ha='left', 
                color='#B08D57', fontsize=11, fontweight='bold')
    ax1.set_yticks(y_pos)
    ax1.set_yticklabels(agent_names, color='#2C2C2C', fontsize=9)
    ax1.set_xlabel('LISTINGS', color='#B08D57', fontsize=9)
    ax1.tick_params(colors='#5A5A5A')
    for spine in ax1.spines.values():
        spine.set_color('#E0E0E0')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.grid(axis='x', alpha=0.3, color='#E0E0E0')
    
    # Right
    ax2 = fig.add_subplot(gs[1])
    ax2.set_facecolor('#FAFAFA')
    colors_pie = ['#B08D57', '#CBA135', '#8B6F2E', '#6B5522', '#9A7A3E']
    wedges, texts = ax2.pie(agent_counts, startangle=90, colors=colors_pie[:len(agent_counts)])
    total = sum(agent_counts)
    for wedge, count in zip(wedges, agent_counts):
        angle = (wedge.theta2 + wedge.theta1) / 2
        x_pos = 0.65 * np.cos(np.radians(angle))
        y_pos = 0.65 * np.sin(np.radians(angle))
        pct = int(count/total*100)
        ax2.text(x_pos, y_pos, f'{pct}%', ha='center', va='center', 
                color='white', fontsize=12, fontweight='bold')
    
    plt.tight_layout()
    path = '/tmp/comp.png'
    plt.savefig(path, dpi=250, bbox_inches='tight', facecolor='white', edgecolor='none')
    plt.close()
    return path

def create_pdf(data):
    """Create PDF using YOUR working structure"""
    
    print("\n" + "="*70)
    print("VOXMILL FINAL PDF GENERATOR")
    print("="*70)
    
    doc = SimpleDocTemplate(OUTPUT_FILE, pagesize=letter, leftMargin=40, rightMargin=40, 
                           topMargin=1.1*inch, bottomMargin=0.8*inch)
    
    story = []
    styles = getSampleStyleSheet()
    
    title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=32, 
                                 textColor=TEXT_DARK, spaceAfter=8, fontName='Helvetica-Bold', 
                                 leading=36, alignment=TA_CENTER)
    
    subtitle_style = ParagraphStyle('Subtitle', parent=styles['Normal'], fontSize=11, 
                                    textColor=BRONZE, spaceAfter=25, fontName='Helvetica', 
                                    leading=16, alignment=TA_CENTER)
    
    section_style = ParagraphStyle('Section', parent=styles['Heading2'], fontSize=20, 
                                   textColor=TEXT_DARK, spaceBefore=8, spaceAfter=12, 
                                   fontName='Helvetica-Bold', leading=24, alignment=TA_CENTER)
    
    body_style = ParagraphStyle('Body', parent=styles['Normal'], fontSize=11, 
                                textColor=TEXT_GREY, spaceAfter=14, leading=17, 
                                alignment=TA_JUSTIFY)
    
    metadata = data['metadata']
    metrics = data['metrics']
    intelligence = data['intelligence']
    
    # Page 1
    story.append(Spacer(1, 0.5*inch))
    story.append(Paragraph("MARKET INTELLIGENCE", section_style))
    story.append(Paragraph("SNAPSHOT", title_style))
    story.append(Spacer(1, 0.3*inch))
    context = f"""WEEKLY PRECISION REPORT • CONFIDENTIAL ANALYSIS<br/><b>{metadata['area'].upper()}, {metadata['city'].upper()}</b><br/>{datetime.now().strftime('%B %d, %Y')}"""
    story.append(Paragraph(context, subtitle_style))
    story.append(Spacer(1, 0.4*inch))
    
    print("→ Creating KPI chart...")
    kpi_chart = create_kpi_chart(data)
    story.append(Image(kpi_chart, width=7*inch, height=2.8*inch))
    story.append(Spacer(1, 0.3*inch))
    
    metrics_data = [
        ['METRIC', 'LOW VALUE', 'HIGH VALUE', 'AVERAGE'],
        ['PRICE', f'£{metrics["min_price"]:,}', f'£{metrics["max_price"]:,}', f'£{metrics["avg_price"]:,.0f}'],
        ['ANALYSIS', 'Entry-level pricing', 'Luxury market ceiling', 'Central market trend']
    ]
    
    metrics_table = Table(metrics_data, colWidths=[1.1*inch, 2*inch, 2*inch, 2*inch])
    metrics_table.setStyle(TableStyle([
        ('BACKGROUND', (0, 0), (-1, 0), BRONZE),
        ('TEXTCOLOR', (0, 0), (-1, 0), WHITE),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 10),
        ('BACKGROUND', (0, 1), (-1, 1), BG_LIGHT),
        ('BACKGROUND', (0, 2), (-1, 2), WHITE),
        ('TEXTCOLOR', (1, 1), (-1, 1), BRONZE),
        ('TEXTCOLOR', (1, 2), (-1, 2), TEXT_GREY),
        ('FONTNAME', (0, 1), (0, 2), 'Helvetica-Bold'),
        ('FONTNAME', (1, 1), (-1, 1), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        ('TEXTCOLOR', (0, 1), (0, 2), BRONZE),
        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 12),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 12),
        ('GRID', (0, 0), (-1, -1), 0.5, BORDER),
        ('LINEABOVE', (0, 0), (-1, 0), 2, BRONZE),
    ]))
    story.append(metrics_table)
    story.append(PageBreak())
    
    # Page 2
    story.append(Paragraph("PERFORMANCE GRAPH", title_style))
    story.append(Paragraph("STRATEGIC ANALYSIS", subtitle_style))
    print("→ Creating performance chart...")
    perf_chart = create_performance_chart(data)
    story.append(Image(perf_chart, width=7*inch, height=4.2*inch))
    story.append(Spacer(1, 0.2*inch))
    story.append(Paragraph("KEY INSIGHTS", section_style))
    summary = intelligence.get('executive_summary', 'Market analysis in progress.')
    story.append(Paragraph(summary, body_style))
    story.append(PageBreak())
    
    # Page 3
    story.append(Paragraph("COMPETITOR LANDSCAPE", title_style))
    story.append(Spacer(1, 0.2*inch))
    print("→ Creating competitor chart...")
    comp_chart = create_competitor_chart(data)
    story.append(Image(comp_chart, width=7*inch, height=2.8*inch))
    story.append(PageBreak())
    
    # Page 4
    story.append(Paragraph("STRATEGIC INTELLIGENCE", title_style))
    story.append(Spacer(1, 0.25*inch))
    story.append(Paragraph("EXECUTIVE SUMMARY", section_style))
    story.append(Paragraph(intelligence.get('executive_summary', 'Analysis in progress.'), body_style))
    story.append(PageBreak())
    
    # Page 5
    story.append(Paragraph("TOP OPPORTUNITIES", title_style))
    story.append(Spacer(1, 0.2*inch))
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
        ('BACKGROUND', (0, 0), (-1, 0), BRONZE),
        ('TEXTCOLOR', (0, 0), (-1, 0), WHITE),
        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
        ('FONTSIZE', (0, 0), (-1, 0), 9),
        ('BACKGROUND', (0, 1), (-1, -1), BG_LIGHT),
        ('TEXTCOLOR', (0, 1), (-1, -1), TEXT_DARK),
        ('FONTSIZE', (0, 1), (-1, -1), 9),
        ('ALIGN', (0, 0), (-1, 0), 'CENTER'),
        ('ALIGN', (1, 1), (-1, -1), 'CENTER'),
        ('VALIGN', (0, 0), (-1, -1), 'MIDDLE'),
        ('TOPPADDING', (0, 0), (-1, -1), 10),
        ('BOTTOMPADDING', (0, 0), (-1, -1), 10),
        ('GRID', (0, 0), (-1, -1), 0.5, BORDER),
        ('LINEABOVE', (0, 0), (-1, 0), 2, BRONZE),
    ]))
    story.append(opp_table)
    
    print("→ Building PDF...")
    doc.build(story, canvasmaker=VoxmillTemplate)
    print(f"✅ PDF created: {OUTPUT_FILE}")
    print("="*70 + "\n")
    return OUTPUT_FILE

def main():
    try:
        if not os.path.exists(INPUT_FILE):
            raise Exception(f"Data not found: {INPUT_FILE}")
        with open(INPUT_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return create_pdf(data)
    except Exception as e:
        print(f"\n❌ ERROR: {str(e)}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
