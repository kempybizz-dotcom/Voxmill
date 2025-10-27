"""
VOXMILL PDF GENERATOR — PRODUCTION VERSION
===========================================
Generates luxury Fortune 500-level PDFs from CSV data
Includes: Charts, metrics dashboards, executive intelligence

NO GOOGLE DEPENDENCIES
"""

import os
import csv
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from reportlab.lib.colors import HexColor
from reportlab.lib.utils import ImageReader
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from io import BytesIO

# ============================================================================
# DESIGN SYSTEM
# ============================================================================

COLORS = {
    'black': HexColor('#0A0A0A'),
    'gold': HexColor('#D4AF37'),
    'gold_light': HexColor('#F4E4A8'),
    'white': HexColor('#FFFFFF'),
    'grey_dark': HexColor('#1A1A1A'),
    'grey_light': HexColor('#666666'),
    'red': HexColor('#FF4444'),
    'green': HexColor('#44FF88')
}

INPUT_DIR = os.environ.get('OUTPUT_DIR', '/tmp')
OUTPUT_DIR = os.environ.get('OUTPUT_DIR', '/tmp')

# ============================================================================
# DATA LOADING
# ============================================================================

def load_data():
    """Load data from CSV files"""
    print("📊 Loading data from CSV...")
    
    report_path = f"{INPUT_DIR}/voxmill_report.csv"
    properties_path = f"{INPUT_DIR}/voxmill_properties.csv"
    
    # Load report summary
    with open(report_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        report = next(reader)
    
    # Load properties
    properties = []
    with open(properties_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            properties.append(row)
    
    print(f"   ✅ Loaded: {len(properties)} properties")
    return report, properties

# ============================================================================
# CHART GENERATION
# ============================================================================

def create_metrics_dashboard(report, properties):
    """Generate 4-panel metrics dashboard"""
    print("📈 Generating metrics dashboard...", end=" ")
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(8, 6), facecolor='#0A0A0A')
    
    # Panel 1: Total Listings
    ax1.set_facecolor('#1A1A1A')
    ax1.axis('off')
    total = report.get('Total Listings', '0')
    ax1.text(0.5, 0.6, total, ha='center', va='center', 
             color='#D4AF37', fontsize=48, fontweight='bold')
    ax1.text(0.5, 0.3, 'TOTAL LISTINGS', ha='center', va='center',
             color='#FFFFFF', fontsize=12, fontweight='bold')
    
    # Panel 2: Avg Price
    ax2.set_facecolor('#1A1A1A')
    ax2.axis('off')
    avg_price = report.get('Avg Price', '£0')
    ax2.text(0.5, 0.6, avg_price, ha='center', va='center',
             color='#D4AF37', fontsize=32, fontweight='bold')
    ax2.text(0.5, 0.3, 'AVG PRICE', ha='center', va='center',
             color='#FFFFFF', fontsize=12, fontweight='bold')
    
    # Panel 3: Deal Status Pie Chart
    ax3.set_facecolor('#0A0A0A')
    hot = int(report.get('Hot Deals', '0'))
    total_count = len(properties)
    other = total_count - hot if total_count > hot else 0
    
    if hot + other > 0:
        sizes = [hot, other]
        colors = ['#44FF88', '#666666']
        labels = ['HOT', 'OTHER']
        wedges, texts, autotexts = ax3.pie(sizes, labels=labels, colors=colors,
                                             autopct='%1.0f%%', startangle=90,
                                             textprops={'color': '#FFFFFF', 'fontweight': 'bold'})
        ax3.set_title('DEAL STATUS', color='#FFFFFF', fontsize=12, fontweight='bold', pad=10)
    
    # Panel 4: Top Deal Scores
    ax4.set_facecolor('#0A0A0A')
    top_scores = []
    for p in properties[:5]:
        try:
            score = float(p.get('deal_score', 0))
            if score > 0:
                top_scores.append(score)
        except:
            continue
    
    if top_scores:
        colors_bars = []
        for score in top_scores:
            if score >= 8:
                colors_bars.append('#44FF88')
            elif score >= 6:
                colors_bars.append('#D4AF37')
            else:
                colors_bars.append('#666666')
        
        y_pos = range(len(top_scores))
        ax4.barh(y_pos, top_scores, color=colors_bars, edgecolor='#F4E4A8', linewidth=1.5)
        ax4.set_xlabel('Score', color='#FFFFFF', fontweight='bold', fontsize=9)
        ax4.set_title('TOP 5 SCORES', color='#FFFFFF', fontsize=11, fontweight='bold', pad=8)
        ax4.set_yticks(y_pos)
        ax4.set_yticklabels([f"#{i+1}" for i in range(len(top_scores))], 
                            color='#FFFFFF', fontsize=9)
        ax4.tick_params(axis='x', colors='#FFFFFF', labelsize=8)
        ax4.spines['bottom'].set_color('#D4AF37')
        ax4.spines['left'].set_color('#D4AF37')
        ax4.spines['top'].set_visible(False)
        ax4.spines['right'].set_visible(False)
        ax4.set_xlim(0, 10)
        ax4.grid(axis='x', alpha=0.2, color='#666666', linewidth=0.5)
    
    plt.tight_layout()
    
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=150, facecolor='#0A0A0A')
    buffer.seek(0)
    plt.close()
    
    print("✅")
    return buffer

def create_deal_ranking_chart(properties):
    """Generate horizontal bar chart for deal rankings"""
    print("📈 Generating deal ranking chart...", end=" ")
    
    top_props = properties[:5]
    if not top_props:
        return None
    
    fig, ax = plt.subplots(figsize=(8, 4), facecolor='#0A0A0A')
    ax.set_facecolor('#0A0A0A')
    
    scores = []
    labels = []
    for i, p in enumerate(top_props):
        try:
            score = float(p.get('deal_score', 0))
            if score > 0:
                scores.append(score)
                labels.append(f"Property {len(scores)}")
        except:
            continue
    
    if not scores:
        plt.close()
        return None
    
    y_pos = range(len(scores))
    colors = ['#44FF88' if s >= 8 else '#D4AF37' if s >= 6 else '#666666' for s in scores]
    
    bars = ax.barh(y_pos, scores, color=colors, edgecolor='#F4E4A8', linewidth=2)
    
    ax.set_xlabel('Deal Score (1-10)', color='#FFFFFF', fontsize=12, fontweight='bold')
    ax.set_title('TOP PROPERTIES — DEAL SCORE RANKING', 
                 color='#D4AF37', fontsize=14, fontweight='bold', pad=20)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, color='#FFFFFF')
    ax.tick_params(axis='x', colors='#FFFFFF')
    ax.spines['bottom'].set_color('#D4AF37')
    ax.spines['left'].set_color('#D4AF37')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.set_xlim(0, 10)
    ax.grid(axis='x', alpha=0.2, color='#666666')
    
    for bar, score in zip(bars, scores):
        ax.text(score + 0.2, bar.get_y() + bar.get_height()/2.,
                f'{score}/10', va='center', color='#FFFFFF', fontweight='bold')
    
    green_patch = mpatches.Patch(color='#44FF88', label='HOT (8+)')
    gold_patch = mpatches.Patch(color='#D4AF37', label='GOOD (6-8)')
    grey_patch = mpatches.Patch(color='#666666', label='WATCH (<6)')
    ax.legend(handles=[green_patch, gold_patch, grey_patch], 
              loc='lower right', facecolor='#1A1A1A', edgecolor='#D4AF37',
              labelcolor='#FFFFFF', fontsize=9)
    
    plt.tight_layout()
    
    buffer = BytesIO()
    plt.savefig(buffer, format='png', dpi=150, facecolor='#0A0A0A')
    buffer.seek(0)
    plt.close()
    
    print("✅")
    return buffer

# ============================================================================
# PDF GENERATION
# ============================================================================

def create_pdf(report, properties, output_path):
    """Generate luxury Fortune 500-level PDF"""
    print(f"\n🎨 Generating PDF: {output_path}")
    
    c = canvas.Canvas(output_path, pagesize=A4)
    width, height = A4
    
    # ========== PAGE 1: COVER + BLUF ==========
    
    c.setFillColor(COLORS['black'])
    c.rect(0, 0, width, height, fill=True, stroke=False)
    
    # Gold header bar
    c.setFillColor(COLORS['gold'])
    c.rect(0, height - 1.5*inch, width, 1.5*inch, fill=True, stroke=False)
    
    c.setFillColor(COLORS['black'])
    c.setFont("Helvetica-Bold", 32)
    c.drawCentredString(width/2, height - 0.8*inch, "VOXMILL")
    c.setFont("Helvetica", 16)
    c.drawCentredString(width/2, height - 1.1*inch, "MARKET INTELLIGENCE")
    
    # Market details
    c.setFillColor(COLORS['white'])
    c.setFont("Helvetica-Bold", 20)
    market = report.get('Market', 'London')
    c.drawCentredString(width/2, height - 2.3*inch, market.upper())
    
    c.setFont("Helvetica", 12)
    timestamp = report.get('Timestamp', datetime.now().strftime('%d %B %Y'))
    c.drawCentredString(width/2, height - 2.7*inch, f"Week of {timestamp}")
    
    # Divider
    c.setStrokeColor(COLORS['gold'])
    c.setLineWidth(2)
    c.line(1*inch, height - 3*inch, width - 1*inch, height - 3*inch)
    
    # BLUF Section
    c.setFillColor(COLORS['gold'])
    c.setFont("Helvetica-Bold", 16)
    c.drawString(1*inch, height - 3.5*inch, "🎯 BLUF (BOTTOM LINE UP FRONT)")
    
    # BLUF box
    c.setFillColor(COLORS['grey_dark'])
    c.rect(1*inch, height - 6.5*inch, width - 2*inch, 2.7*inch, fill=True, stroke=False)
    c.setStrokeColor(COLORS['gold'])
    c.setLineWidth(3)
    c.rect(1*inch, height - 6.5*inch, width - 2*inch, 2.7*inch, fill=False, stroke=True)
    
    # BLUF content
    bluf = report.get('BLUF', 'Intelligence summary not available')
    c.setFillColor(COLORS['white'])
    c.setFont("Helvetica", 10)
    
    y_pos = height - 4*inch
    max_width = width - 2.5*inch
    
    for line in bluf.split('\n'):
        if line.strip():
            words = line.split()
            current_line = []
            for word in words:
                test = ' '.join(current_line + [word])
                if c.stringWidth(test, "Helvetica", 10) < max_width:
                    current_line.append(word)
                else:
                    if current_line:
                        c.drawString(1.2*inch, y_pos, ' '.join(current_line))
                        y_pos -= 0.18*inch
                    current_line = [word]
            if current_line:
                c.drawString(1.2*inch, y_pos, ' '.join(current_line))
                y_pos -= 0.18*inch
    
    # Key Metrics
    c.setFillColor(COLORS['gold'])
    c.setFont("Helvetica-Bold", 14)
    c.drawString(1*inch, height - 7*inch, "📊 KEY METRICS")
    
    c.setFillColor(COLORS['white'])
    c.setFont("Helvetica", 11)
    y_metrics = height - 7.4*inch
    
    c.drawString(1.2*inch, y_metrics, f"• {report.get('Total Listings', '0')} Active Listings")
    c.drawString(1.2*inch, y_metrics - 0.25*inch, f"• Avg Price: {report.get('Avg Price', '£0')}")
    c.drawString(1.2*inch, y_metrics - 0.5*inch, f"• {report.get('Hot Deals', '0')} Hot Deals (8+/10)")
    
    # Footer
    c.setFillColor(COLORS['gold'])
    c.setFont("Helvetica", 9)
    c.drawString(1*inch, 0.7*inch, "Prepared by: Olly Kempster")
    c.drawString(1*inch, 0.5*inch, "info@voxmill.co.uk")
    c.setFillColor(COLORS['grey_light'])
    c.drawRightString(width - 1*inch, 0.5*inch, "© Voxmill Intelligence 2025")
    
    c.showPage()
    
    # ========== PAGE 2: CHARTS ==========
    
    c.setFillColor(COLORS['black'])
    c.rect(0, 0, width, height, fill=True, stroke=False)
    
    c.setFillColor(COLORS['gold'])
    c.setFont("Helvetica-Bold", 24)
    c.drawString(1*inch, height - 1*inch, "MARKET ANALYTICS")
    
    c.setStrokeColor(COLORS['gold'])
    c.setLineWidth(2)
    c.line(1*inch, height - 1.2*inch, width - 1*inch, height - 1.2*inch)
    
    # Insert charts
    metrics_chart = create_metrics_dashboard(report, properties)
    if metrics_chart:
        c.drawImage(ImageReader(metrics_chart), 0.8*inch, height - 5*inch, 
                   width=6.5*inch, height=3*inch, preserveAspectRatio=True)
    
    ranking_chart = create_deal_ranking_chart(properties)
    if ranking_chart:
        c.drawImage(ImageReader(ranking_chart), 0.8*inch, height - 8.5*inch,
                   width=6.5*inch, height=3*inch, preserveAspectRatio=True)
    
    c.setFillColor(COLORS['grey_light'])
    c.setFont("Helvetica", 8)
    c.drawCentredString(width/2, 0.5*inch, "Page 2 of 3")
    
    c.showPage()
    
    # ========== PAGE 3: INTELLIGENCE ==========
    
    c.setFillColor(COLORS['black'])
    c.rect(0, 0, width, height, fill=True, stroke=False)
    
    c.setFillColor(COLORS['gold'])
    c.setFont("Helvetica-Bold", 24)
    c.drawString(1*inch, height - 1*inch, "STRATEGIC INTELLIGENCE")
    
    c.setStrokeColor(COLORS['gold'])
    c.setLineWidth(2)
    c.line(1*inch, height - 1.2*inch, width - 1*inch, height - 1.2*inch)
    
    # Opportunities
    c.setFillColor(COLORS['gold'])
    c.setFont("Helvetica-Bold", 14)
    c.drawString(1*inch, height - 1.7*inch, "💎 TOP OPPORTUNITIES")
    
    opportunities = report.get('Top Opportunities', 'No opportunities available')
    c.setFillColor(COLORS['white'])
    c.setFont("Helvetica", 9)
    
    y_pos = height - 2.1*inch
    for line in opportunities.split('\n')[:18]:
        if line.strip() and y_pos > height - 5*inch:
            c.drawString(1.2*inch, y_pos, line[:85])
            y_pos -= 0.16*inch
    
    # Risks
    c.setFillColor(COLORS['red'])
    c.setFont("Helvetica-Bold", 14)
    c.drawString(1*inch, height - 5.5*inch, "⚠️ RISK ASSESSMENT")
    
    risks = report.get('Risk Assessment', 'No risks identified')
    c.setFillColor(COLORS['white'])
    c.setFont("Helvetica", 9)
    
    y_pos = height - 5.9*inch
    for line in risks.split('\n')[:12]:
        if line.strip() and y_pos > height - 7.5*inch:
            c.drawString(1.2*inch, y_pos, line[:85])
            y_pos -= 0.16*inch
    
    # Action Triggers
    c.setFillColor(COLORS['gold'])
    c.setFont("Helvetica-Bold", 14)
    c.drawString(1*inch, height - 8*inch, "⚡ ACTION TRIGGERS")
    
    triggers = report.get('Action Triggers', 'No triggers defined')
    c.setFillColor(COLORS['white'])
    c.setFont("Helvetica", 9)
    
    y_pos = height - 8.4*inch
    for line in triggers.split('\n')[:8]:
        if line.strip() and y_pos > height - 9.5*inch:
            c.drawString(1.2*inch, y_pos, line[:85])
            y_pos -= 0.16*inch
    
    # CTA
    c.setFillColor(COLORS['gold'])
    c.rect(0.5*inch, 0.8*inch, width - 1*inch, 0.6*inch, fill=True, stroke=False)
    c.setFillColor(COLORS['black'])
    c.setFont("Helvetica-Bold", 11)
    c.drawCentredString(width/2, 1.05*inch, 
                       "Want weekly intelligence? Contact: info@voxmill.co.uk")
    
    c.setFillColor(COLORS['grey_light'])
    c.setFont("Helvetica", 8)
    c.drawCentredString(width/2, 0.5*inch, "Page 3 of 3")
    
    c.save()
    print(f"   ✅ PDF generated: {output_path}")

# ============================================================================
# MAIN
# ============================================================================

def main():
    print("\n" + "="*70)
    print("VOXMILL PDF GENERATOR")
    print("="*70)
    
    try:
        # Load data
        report, properties = load_data()
        
        # Generate PDF
        pdf_path = f"{OUTPUT_DIR}/voxmill_report.pdf"
        create_pdf(report, properties, pdf_path)
        
        print("\n" + "="*70)
        print("✅ PDF GENERATION COMPLETE")
        print("="*70)
        print(f"\n📄 Output: {pdf_path}")
        print("\n🎨 Features:")
        print("   • Black-and-gold Fortune 500 design")
        print("   • BLUF executive summary")
        print("   • Metrics dashboard with charts")
        print("   • Deal score rankings")
        print("   • Strategic intelligence (opportunities, risks, triggers)")
        print("   • 3 pages of professional analysis")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()
        raise

if __name__ == "__main__":
    main()
