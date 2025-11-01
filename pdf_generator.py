"""
VOXMILL EXECUTIVE PDF GENERATOR
================================
Fortune-500 black/bronze design system
Full functionality preserved
"""

import os
import json
from datetime import datetime
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer, PageBreak, Image
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER, TA_JUSTIFY, TA_RIGHT
from reportlab.pdfgen import canvas
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np

INPUT_FILE = "/tmp/voxmill_analysis.json"
OUTPUT_FILE = "/tmp/Voxmill_Elite_Intelligence.pdf"
LOGO_PATH = "/mnt/user-data/uploads/voxmill_logo.png"

# VOXMILL DESIGN SYSTEM - EXACT COLORS
BG_BLACK = colors.HexColor('#0B0B0B')
BG_PANEL = colors.HexColor('#121212')
BG_PANEL_ALT = colors.HexColor('#161616')
BRONZE = colors.HexColor('#B08D57')
GOLD = colors.HexColor('#CBA135')
LINE = colors.HexColor('#2E2E2E')
TEXT_LIGHT = colors.HexColor('#EAEAEA')
TEXT_MUTED = colors.HexColor('#AFAFAF')

class VoxmillCanvas(canvas.Canvas):
    """Executive canvas with Voxmill branding"""
    
    def __init__(self, *args, **kwargs):
        canvas.Canvas.__init__(self, *args, **kwargs)
        self.pages = []
        self.logo_path = LOGO_PATH if os.path.exists(LOGO_PATH) else None
        
    def showPage(self):
        self.pages.append(dict(self.__dict__))
        self._startPage()
        
    def save(self):
        page_count = len(self.pages)
        for i, page in enumerate(self.pages):
            self.__dict__.update(page)
            self.draw_template(i + 1, page_count)
            canvas.Canvas.showPage(self)
        canvas.Canvas.save(self)
        
    def draw_template(self, page_num, page_count):
        """Draw header and footer"""
        
        # Black background
        self.setFillColor(BG_BLACK)
        self.rect(0, 0, 8.5*inch, 11*inch, fill=1, stroke=0)
        
        # Top gold line
        self.setStrokeColor(GOLD)
        self.setLineWidth(1)
        self.line(50, 10.6*inch, 8*inch, 10.6*inch)
        
        # Logo
        if self.logo_path:
            try:
                self.drawImage(self.logo_path, 4.25*inch - 25, 10.68*inch, 
                             width=50, height=50, preserveAspectRatio=True, mask='auto')
            except:
                pass
        
        # Brand text
        self.setFont('Times-Bold', 10)
        self.setFillColor(BRONZE)
        self.drawCentredString(4.25*inch, 10.45*inch, "VOXMILL MARKET INTELLIGENCE")
        
        # Footer
        self.setStrokeColor(GOLD)
        self.setLineWidth(1)
        self.line(50, 0.6*inch, 8*inch, 0.6*inch)
        
        self.setFont('Helvetica', 8)
        self.setFillColor(BRONZE)
        footer_text = f"VOXMILL AUTOMATIONS — CONFIDENTIAL | {datetime.now().year}"
        self.drawCentredString(4.25*inch, 0.42*inch, footer_text)
        
        self.setFillColor(TEXT_MUTED)
        self.drawRightString(8*inch, 0.42*inch, f"PAGE {page_num} OF {page_count}")

def create_chart_kpi(data):
    """KPI chart - black background, bronze bars"""
    
    metrics = data['metrics']
    properties = data['properties']
    
    exceptional = sum(1 for p in properties if p.get('deal_score', 0) >= 9)
    hot = sum(1 for p in properties if 7 <= p.get('deal_score', 0) < 9)
    strong = sum(1 for p in properties if 5 <= p.get('deal_score', 0) < 7)
    
    plt.style.use('dark_background')
    fig = plt.figure(figsize=(10, 3.5), facecolor='#0B0B0B')
    gs = fig.add_gridspec(1, 2, width_ratios=[1, 2], wspace=0.3)
    
    # Left - total analyzed
    ax1 = fig.add_subplot(gs[0])
    ax1.set_facecolor('#0B0B0B')
    ax1.bar([0], [metrics['total_properties']], width=0.4, 
            color='#B08D57', alpha=0.9, edgecolor='#CBA135', linewidth=1.5)
    ax1.set_xlim(-0.5, 0.5)
    ax1.set_ylim(0, metrics['total_properties'] * 1.15)
    ax1.set_xticks([0])
    ax1.set_xticklabels(['ANALYZED'], color='#AFAFAF', fontsize=10, weight='600')
    ax1.set_ylabel('PROPERTIES', color='#B08D57', fontsize=9, weight='600')
    ax1.tick_params(colors='#AFAFAF', width=0)
    for spine in ax1.spines.values():
        spine.set_color('#2E2E2E')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.grid(axis='y', alpha=0.15, color='#2E2E2E', linestyle='-', linewidth=0.5)
    
    # Right - deal tiers
    ax2 = fig.add_subplot(gs[1])
    ax2.set_facecolor('#0B0B0B')
    categories = ['EXCEPTIONAL\nDEALS', 'HOT\nDEALS', 'STRONG\nVALUE']
    values = [exceptional, hot, strong]
    ax2.barh(categories, values, color='#B08D57', alpha=0.9, 
             edgecolor='#CBA135', linewidth=1.5)
    max_val = max(values) if values else 1
    for i, val in enumerate(values):
        ax2.text(val + max_val*0.02, i, str(val), va='center', ha='left', 
                color='#CBA135', fontsize=11, weight='bold')
    ax2.set_xlabel('COUNT', color='#B08D57', fontsize=9, weight='600')
    ax2.tick_params(colors='#AFAFAF', width=0)
    for spine in ax2.spines.values():
        spine.set_color('#2E2E2E')
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.grid(axis='x', alpha=0.15, color='#2E2E2E', linestyle='-', linewidth=0.5)
    
    plt.tight_layout()
    path = '/tmp/kpi.png'
    plt.savefig(path, dpi=200, bbox_inches='tight', facecolor='#0B0B0B', edgecolor='none')
    plt.close()
    return path

def create_chart_performance(data):
    """Performance charts - black background"""
    
    properties = data['properties'][:10]
    
    plt.style.use('dark_background')
    fig = plt.figure(figsize=(10, 5.5), facecolor='#0B0B0B')
    gs = fig.add_gridspec(2, 2, hspace=0.3, wspace=0.3)
    
    # Top left - prices
    ax1 = fig.add_subplot(gs[0, 0])
    ax1.set_facecolor('#0B0B0B')
    prices = [p['price']/1e6 for p in properties[:6]]
    addresses = [p['address'].split(',')[0][:15] for p in properties[:6]]
    ax1.barh(range(len(prices)), prices, color='#B08D57', alpha=0.9, edgecolor='#CBA135', linewidth=1)
    ax1.set_yticks(range(len(addresses)))
    ax1.set_yticklabels(addresses, color='#AFAFAF', fontsize=8)
    ax1.set_xlabel('PRICE (£M)', color='#B08D57', fontsize=8, weight='600')
    ax1.tick_params(colors='#AFAFAF', width=0)
    for spine in ax1.spines.values():
        spine.set_color('#2E2E2E')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.grid(axis='x', alpha=0.15, color='#2E2E2E')
    
    # Top right - trend
    ax2 = fig.add_subplot(gs[0, 1])
    ax2.set_facecolor('#0B0B0B')
    x = list(range(len(properties)))
    y = [p['price_per_sqft'] for p in properties]
    ax2.plot(x, y, color='#B08D57', linewidth=2.5, marker='o', markersize=6, 
             markerfacecolor='#B08D57', markeredgecolor='#CBA135', markeredgewidth=1.5, alpha=0.9)
    ax2.fill_between(x, y, alpha=0.2, color='#CBA135')
    ax2.set_xlabel('PROPERTY RANK', color='#B08D57', fontsize=8, weight='600')
    ax2.set_ylabel('£ / SQFT', color='#B08D57', fontsize=8, weight='600')
    ax2.tick_params(colors='#AFAFAF', width=0)
    for spine in ax2.spines.values():
        spine.set_color('#2E2E2E')
    ax2.spines['top'].set_visible(False)
    ax2.spines['right'].set_visible(False)
    ax2.grid(alpha=0.15, color='#2E2E2E')
    
    # Bottom left - types
    ax3 = fig.add_subplot(gs[1, 0])
    ax3.set_facecolor('#0B0B0B')
    types = {}
    for p in properties:
        t = p['property_type']
        types[t] = types.get(t, 0) + 1
    colors_pie = ['#B08D57', '#CBA135', '#8B6F2E', '#6B5522']
    wedges, _ = ax3.pie(types.values(), startangle=90, colors=colors_pie[:len(types)])
    for wedge in wedges:
        wedge.set_alpha(0.9)
    total = sum(types.values())
    for wedge, count in zip(wedges, types.values()):
        ang = (wedge.theta2 + wedge.theta1)/2
        x_p = 0.6*np.cos(np.radians(ang))
        y_p = 0.6*np.sin(np.radians(ang))
        ax3.text(x_p, y_p, f'{int(count/total*100)}%', ha='center', va='center', 
                color='white', fontsize=11, weight='bold')
    ax3.legend(types.keys(), loc='lower left', fontsize=7, facecolor='#121212', 
              edgecolor='#2E2E2E', labelcolor='#EAEAEA')
    
    # Bottom right - scores
    ax4 = fig.add_subplot(gs[1, 1])
    ax4.set_facecolor('#0B0B0B')
    scores = [p['deal_score'] for p in properties]
    colors_bars = ['#B08D57' if s>=8 else '#CBA135' if s>=7 else '#8B6F2E' for s in scores]
    ax4.bar(range(len(scores)), scores, color=colors_bars, alpha=0.9, edgecolor='#CBA135', linewidth=1)
    ax4.axhline(8, color='#B08D57', linestyle='--', linewidth=1, alpha=0.3)
    ax4.axhline(7, color='#CBA135', linestyle='--', linewidth=1, alpha=0.3)
    ax4.set_xlabel('PROPERTY', color='#B08D57', fontsize=8, weight='600')
    ax4.set_ylabel('DEAL SCORE', color='#B08D57', fontsize=8, weight='600')
    ax4.set_ylim(0, 10)
    ax4.tick_params(colors='#AFAFAF', width=0)
    for spine in ax4.spines.values():
        spine.set_color('#2E2E2E')
    ax4.spines['top'].set_visible(False)
    ax4.spines['right'].set_visible(False)
    ax4.grid(axis='y', alpha=0.15, color='#2E2E2E')
    
    plt.tight_layout()
    path = '/tmp/perf.png'
    plt.savefig(path, dpi=200, bbox_inches='tight', facecolor='#0B0B0B', edgecolor='none')
    plt.close()
    return path

def create_chart_competitors(data):
    """Competitor chart - black background"""
    
    properties = data['properties']
    agents = {}
    for p in properties:
        agent = p.get('agent', 'Private')[:18]
        agents[agent] = agents.get(agent, 0) + 1
    top = sorted(agents.items(), key=lambda x: x[1], reverse=True)[:5]
    
    plt.style.use('dark_background')
    fig = plt.figure(figsize=(10, 3.5), facecolor='#0B0B0B')
    gs = fig.add_gridspec(1, 2, width_ratios=[1.2, 1], wspace=0.3)
    
    # Left - bars
    ax1 = fig.add_subplot(gs[0])
    ax1.set_facecolor('#0B0B0B')
    names = [a[0] for a in top]
    counts = [a[1] for a in top]
    ax1.barh(range(len(names)), counts, color='#B08D57', alpha=0.9, edgecolor='#CBA135', linewidth=1.5)
    for i, c in enumerate(counts):
        ax1.text(c+0.3, i, str(c), va='center', color='#CBA135', fontsize=10, weight='bold')
    ax1.set_yticks(range(len(names)))
    ax1.set_yticklabels(names, color='#AFAFAF', fontsize=8)
    ax1.set_xlabel('LISTINGS', color='#B08D57', fontsize=8, weight='600')
    ax1.tick_params(colors='#AFAFAF', width=0)
    for spine in ax1.spines.values():
        spine.set_color('#2E2E2E')
    ax1.spines['top'].set_visible(False)
    ax1.spines['right'].set_visible(False)
    ax1.grid(axis='x', alpha=0.15, color='#2E2E2E')
    
    # Right - pie
    ax2 = fig.add_subplot(gs[1])
    ax2.set_facecolor('#0B0B0B')
    cols = ['#B08D57', '#CBA135', '#8B6F2E', '#6B5522', '#9A7A3E']
    wedges, _ = ax2.pie(counts, startangle=90, colors=cols[:len(counts)])
    for wedge in wedges:
        wedge.set_alpha(0.9)
    total = sum(counts)
    for wedge, c in zip(wedges, counts):
        ang = (wedge.theta2 + wedge.theta1)/2
        x_p = 0.6*np.cos(np.radians(ang))
        y_p = 0.6*np.sin(np.radians(ang))
        ax2.text(x_p, y_p, f'{int(c/total*100)}%', ha='center', va='center', 
                color='white', fontsize=11, weight='bold')
    
    plt.tight_layout()
    path = '/tmp/comp.png'
    plt.savefig(path, dpi=200, bbox_inches='tight', facecolor='#0B0B0B', edgecolor='none')
    plt.close()
    return path

def create_pdf(data):
    """Create executive PDF"""
    
    print("\n" + "="*70)
    print("VOXMILL EXECUTIVE PDF GENERATOR")
    print("="*70)
    
    doc = SimpleDocTemplate(OUTPUT_FILE, pagesize=letter, leftMargin=50, rightMargin=50, 
                           topMargin=0.95*inch, bottomMargin=0.8*inch)
    
    story = []
    styles = getSampleStyleSheet()
    
    # Styles - Playfair + Inter
    title_style = ParagraphStyle('Title', parent=styles['Heading1'], fontSize=26, 
                                 textColor=TEXT_LIGHT, spaceAfter=6, fontName='Times-Bold', 
                                 leading=30, alignment=TA_CENTER, spaceBefore=20)
    
    subtitle_style = ParagraphStyle('Subtitle', parent=styles['Normal'], fontSize=14, 
                                    textColor=BRONZE, spaceAfter=16, fontName='Helvetica-Bold', 
                                    leading=18, alignment=TA_CENTER, wordSpace=1.5)
    
    section_style = ParagraphStyle('Section', parent=styles['Heading2'], fontSize=18, 
                                   textColor=BRONZE, spaceBefore=20, spaceAfter=14, 
                                   fontName='Times-Bold', leading=22, alignment=TA_CENTER)
    
    body_style = ParagraphStyle('Body', parent=styles['Normal'], fontSize=12, 
                                textColor=TEXT_LIGHT, spaceAfter=14, leading=18, 
                                alignment=TA_JUSTIFY)
    
    meta = data['metadata']
    metrics = data['metrics']
    intel = data['intelligence']
    
    # Page 1
    story.append(Spacer(1, 0.25*inch))
    story.append(Paragraph("MARKET INTELLIGENCE", section_style))
    story.append(Paragraph("SNAPSHOT", title_style))
    story.append(Spacer(1, 0.12*inch))
    ctx = f"""<font size="11">WEEKLY EXECUTIVE REPORT — CONFIDENTIAL</font><br/><b>{meta['area'].upper()}, {meta['city'].upper()}</b><br/><font size="11">{datetime.now().strftime('%B %d, %Y')}</font>"""
    story.append(Paragraph(ctx, subtitle_style))
    story.append(Spacer(1, 0.2*inch))
    
    print("→ Creating KPI chart...")
    kpi = create_chart_kpi(data)
    story.append(Image(kpi, width=6.8*inch, height=2.4*inch))
    story.append(Spacer(1, 0.18*inch))
    
    tbl_data = [
        ['METRIC', 'LOW VALUE', 'HIGH VALUE', 'AVERAGE'],
        ['PRICE', f'£{metrics["min_price"]:,}', f'£{metrics["max_price"]:,}', f'£{metrics["avg_price"]:,.0f}'],
        ['ANALYSIS', 'Entry threshold', 'Premium ceiling', 'Market trend']
    ]
    
    tbl = Table(tbl_data, colWidths=[1*inch, 1.9*inch, 1.9*inch, 1.9*inch])
    tbl.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), BRONZE),
        ('TEXTCOLOR', (0,0), (-1,0), BG_BLACK),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,0), 10),
        ('BACKGROUND', (0,1), (-1,1), BG_PANEL),
        ('BACKGROUND', (0,2), (-1,2), BG_PANEL_ALT),
        ('TEXTCOLOR', (1,1), (-1,1), GOLD),
        ('TEXTCOLOR', (1,2), (-1,2), TEXT_LIGHT),
        ('FONTNAME', (0,1), (0,2), 'Helvetica-Bold'),
        ('FONTNAME', (1,1), (-1,1), 'Helvetica-Bold'),
        ('FONTSIZE', (0,1), (-1,-1), 9),
        ('TEXTCOLOR', (0,1), (0,2), BRONZE),
        ('ALIGN', (0,0), (0,-1), 'CENTER'),
        ('ALIGN', (1,0), (-1,-1), 'RIGHT'),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('TOPPADDING', (0,0), (-1,-1), 10),
        ('BOTTOMPADDING', (0,0), (-1,-1), 10),
        ('LINEBELOW', (0,0), (-1,0), 2, GOLD),
        ('LINEABOVE', (0,1), (-1,1), 0.5, LINE),
        ('LINEABOVE', (0,2), (-1,2), 0.5, LINE),
    ]))
    story.append(tbl)
    story.append(PageBreak())
    
    # Page 2
    story.append(Paragraph("PERFORMANCE ANALYSIS", title_style))
    story.append(Spacer(1, 0.12*inch))
    print("→ Creating performance chart...")
    perf = create_chart_performance(data)
    story.append(Image(perf, width=6.8*inch, height=3.7*inch))
    story.append(Spacer(1, 0.12*inch))
    story.append(Paragraph("KEY INSIGHTS", section_style))
    summ = intel.get('executive_summary', 'Market analysis in progress.')
    story.append(Paragraph(summ, body_style))
    story.append(PageBreak())
    
    # Page 3
    story.append(Paragraph("COMPETITOR LANDSCAPE", title_style))
    story.append(Spacer(1, 0.12*inch))
    print("→ Creating competitor chart...")
    comp = create_chart_competitors(data)
    story.append(Image(comp, width=6.8*inch, height=2.4*inch))
    story.append(PageBreak())
    
    # Page 4
    story.append(Paragraph("STRATEGIC INTELLIGENCE", title_style))
    story.append(Spacer(1, 0.12*inch))
    story.append(Paragraph("EXECUTIVE SUMMARY", section_style))
    story.append(Paragraph(intel.get('executive_summary', 'Analysis in progress.'), body_style))
    story.append(PageBreak())
    
    # Page 5
    story.append(Paragraph("TOP OPPORTUNITIES", title_style))
    story.append(Spacer(1, 0.12*inch))
    tops = data['top_opportunities'][:8]
    opp_data = [['ADDRESS', 'PRICE', 'BEDS/BATHS', '£/SQFT', 'SCORE']]
    for p in tops:
        opp_data.append([
            p['address'].split(',')[0][:32],
            f"£{p['price']:,}",
            f"{p['beds']}/{p['baths']}",
            f"£{p['price_per_sqft']:,.0f}",
            f"{p['deal_score']}/10"
        ])
    
    opp_tbl = Table(opp_data, colWidths=[2.4*inch, 1.2*inch, 0.85*inch, 0.85*inch, 0.7*inch])
    opp_tbl.setStyle(TableStyle([
        ('BACKGROUND', (0,0), (-1,0), BRONZE),
        ('TEXTCOLOR', (0,0), (-1,0), BG_BLACK),
        ('FONTNAME', (0,0), (-1,0), 'Helvetica-Bold'),
        ('FONTSIZE', (0,0), (-1,0), 9),
        ('BACKGROUND', (0,1), (-1,-1), BG_PANEL),
        ('ROWBACKGROUNDS', (0,1), (-1,-1), [BG_PANEL, BG_PANEL_ALT]),
        ('TEXTCOLOR', (0,1), (-1,-1), TEXT_LIGHT),
        ('FONTSIZE', (0,1), (-1,-1), 9),
        ('ALIGN', (0,0), (0,0), 'CENTER'),
        ('ALIGN', (1,0), (-1,-1), 'RIGHT'),
        ('VALIGN', (0,0), (-1,-1), 'MIDDLE'),
        ('TOPPADDING', (0,0), (-1,-1), 9),
        ('BOTTOMPADDING', (0,0), (-1,-1), 9),
        ('LINEBELOW', (0,0), (-1,0), 2, GOLD),
        ('LINEABOVE', (0,1), (-1,-1), 0.5, LINE),
    ]))
    story.append(opp_tbl)
    
    print("→ Building PDF...")
    doc.build(story, canvasmaker=VoxmillCanvas)
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
