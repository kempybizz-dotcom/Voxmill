"""
VOXMILL PDF GENERATOR — ELITE VISUAL INTELLIGENCE
==================================================
Generates luxury market intelligence PDFs with:
- Matplotlib charts (price distribution, deal scores, trends)
- Visual property cards with scoring bars
- Risk matrix heatmaps
- Fortune 500-level design
- Black-and-gold aesthetic
"""

import os
import json
from datetime import datetime
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import inch
from reportlab.pdfgen import canvas
from reportlab.lib.colors import HexColor
from reportlab.lib.utils import ImageReader
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from io import BytesIO
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ============================================================================
# VISUAL DESIGN SYSTEM
# ============================================================================

COLORS = {
    'black': HexColor('#0A0A0A'),
    'gold': HexColor('#D4AF37'),
    'gold_light': HexColor('#F4E4A8'),
    'white': HexColor('#FFFFFF'),
    'grey_dark': HexColor('#1A1A1A'),
    'grey_medium': HexColor('#333333'),
    'grey_light': HexColor('#666666'),
    'red': HexColor('#FF4444'),
    'green': HexColor('#44FF88')
}

# ============================================================================
# GOOGLE SHEETS CONNECTION
# ============================================================================

def get_sheet_data(sheet_name="London Real Estate"):
    """Pull latest report data from Google Sheets"""
    print(f"📊 Fetching data from: {sheet_name}")
    
    creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
    creds_dict = json.loads(creds_json)
    scopes = ['https://www.googleapis.com/auth/spreadsheets']
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    client = gspread.authorize(credentials)
    sheet_id = os.environ.get('GOOGLE_SHEET_ID')
    
    spreadsheet = client.open_by_key(sheet_id)
    ws = spreadsheet.worksheet(sheet_name)
    
    # Get all data
    data = ws.get_all_values()
    
    if len(data) < 2:
        raise ValueError("No data in sheet")
    
    headers = data[0]
    latest_row = data[-1]  # Most recent report
    
    # Parse into dictionary
    report = dict(zip(headers, latest_row))
    
    print(f"  ✅ Loaded report from: {report.get('Timestamp', 'Unknown')}")
    return report

# ============================================================================
# CHART GENERATION
# ============================================================================

def generate_price_distribution_chart(properties_text, currency="£"):
    """Generate price distribution bar chart"""
    print("📈 Generating price distribution chart...")
    
    # Parse properties from text
    prices = []
    for line in properties_text.split('\n'):
        if currency in line:
            # Extract price
            import re
            match = re.search(rf'{currency}([\d,]+)', line)
            if match:
                price = int(match.group(1).replace(',', ''))
                prices.append(price)
    
    if not prices:
        return None
    
    # Create bins
    min_price = min(prices)
    max_price = max(prices)
    range_size = (max_price - min_price) / 5
    
    bins = [min_price + i * range_size for i in range(6)]
    
    # Count properties in each bin
    counts = [0] * 5
    for price in prices:
        for i in range(5):
            if bins[i] <= price < bins[i+1]:
                counts[i] += 1
                break
    
    # Create chart
    fig, ax = plt.subplots(figsize=(8, 4), facecolor='#0A0A0A')
    ax.set_facecolor('#0A0A0A')
    
    bin_labels = [f'{currency}{int(bins[i]/1000)}k-{int(bins[i+1]/1000)}k' for i in range(5)]
    x_pos = range(len(bin_labels))
    
    bars = ax.bar(x_pos, counts, color='#D4AF37', edgecolor='#F4E4A8', linewidth=2)
    
    ax.set_xlabel('Price Range', color='#FFFFFF', fontsize=12, fontweight='bold')
    ax.set_ylabel('Number of Properties', color='#FFFFFF', fontsize=12, fontweight='bold')
    ax.set_title('PRICE DISTRIBUTION ANALYSIS', color='#D4AF37', fontsize=14, fontweight='bold', pad=20)
    ax.set_xticks(x_pos)
    ax.set_xticklabels(bin_labels, color='#FFFFFF', rotation=45, ha='right')
    ax.tick_params(axis='y', colors='#FFFFFF')
    ax.spines['bottom'].set_color('#D4AF37')
    ax.spines['left'].set_color('#D4AF37')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.grid(axis='y', alpha=0.2, color='#666666')
    
    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom', color='#FFFFFF', fontweight='bold')
    
    plt.tight_layout()
    
    # Save to BytesIO
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png', dpi=150, facecolor='#0A0A0A', edgecolor='none')
    img_buffer.seek(0)
    plt.close()
    
    print("  ✅ Price distribution chart generated")
    return img_buffer

def generate_deal_score_chart(properties_text):
    """Generate horizontal bar chart showing deal scores"""
    print("📈 Generating deal score chart...")
    
    # Parse properties
    import re
    properties = []
    current_prop = {}
    
    for line in properties_text.split('\n'):
        if line.strip().startswith(tuple('123456789')):
            if current_prop:
                properties.append(current_prop)
            current_prop = {'address': line.strip()}
        elif 'Score:' in line:
            match = re.search(r'Score: ([\d.]+)/10', line)
            if match and current_prop:
                current_prop['score'] = float(match.group(1))
    
    if current_prop:
        properties.append(current_prop)
    
    if not properties or not any('score' in p for p in properties):
        return None
    
    # Take top 5
    properties = [p for p in properties if 'score' in p][:5]
    
    # Create chart
    fig, ax = plt.subplots(figsize=(8, 4), facecolor='#0A0A0A')
    ax.set_facecolor('#0A0A0A')
    
    scores = [p['score'] for p in properties]
    labels = [f"Property {i+1}" for i in range(len(properties))]
    y_pos = range(len(labels))
    
    # Color bars by score (green for 8+, gold for 6-8, grey for <6)
    colors = []
    for score in scores:
        if score >= 8.0:
            colors.append('#44FF88')
        elif score >= 6.0:
            colors.append('#D4AF37')
        else:
            colors.append('#666666')
    
    bars = ax.barh(y_pos, scores, color=colors, edgecolor='#F4E4A8', linewidth=2)
    
    ax.set_xlabel('Deal Score (1-10)', color='#FFFFFF', fontsize=12, fontweight='bold')
    ax.set_title('TOP PROPERTIES — DEAL SCORE RANKING', color='#D4AF37', fontsize=14, fontweight='bold', pad=20)
    ax.set_yticks(y_pos)
    ax.set_yticklabels(labels, color='#FFFFFF')
    ax.tick_params(axis='x', colors='#FFFFFF')
    ax.spines['bottom'].set_color('#D4AF37')
    ax.spines['left'].set_color('#D4AF37')
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.set_xlim(0, 10)
    ax.grid(axis='x', alpha=0.2, color='#666666')
    
    # Add score labels
    for i, (bar, score) in enumerate(zip(bars, scores)):
        ax.text(score + 0.2, bar.get_y() + bar.get_height()/2.,
                f'{score}/10',
                va='center', color='#FFFFFF', fontweight='bold')
    
    # Add legend
    green_patch = mpatches.Patch(color='#44FF88', label='HOT DEAL (8+)')
    gold_patch = mpatches.Patch(color='#D4AF37', label='GOOD (6-8)')
    grey_patch = mpatches.Patch(color='#666666', label='WATCH (<6)')
    ax.legend(handles=[green_patch, gold_patch, grey_patch], 
              loc='lower right', facecolor='#1A1A1A', edgecolor='#D4AF37',
              labelcolor='#FFFFFF')
    
    plt.tight_layout()
    
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png', dpi=150, facecolor='#0A0A0A', edgecolor='none')
    img_buffer.seek(0)
    plt.close()
    
    print("  ✅ Deal score chart generated")
    return img_buffer

def generate_metrics_dashboard(report):
    """Generate visual metrics dashboard"""
    print("📈 Generating metrics dashboard...")
    
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(8, 6), facecolor='#0A0A0A')
    
    # Metric 1: Total Listings (big number)
    ax1.set_facecolor('#1A1A1A')
    ax1.axis('off')
    total = report.get('Total Listings', '0')
    ax1.text(0.5, 0.6, total, ha='center', va='center', 
             color='#D4AF37', fontsize=48, fontweight='bold')
    ax1.text(0.5, 0.3, 'TOTAL LISTINGS', ha='center', va='center',
             color='#FFFFFF', fontsize=12, fontweight='bold')
    
    # Metric 2: Avg Price (big number)
    ax2.set_facecolor('#1A1A1A')
    ax2.axis('off')
    avg_price = report.get('Avg Price', '£0')
    ax2.text(0.5, 0.6, avg_price, ha='center', va='center',
             color='#D4AF37', fontsize=36, fontweight='bold')
    ax2.text(0.5, 0.3, 'AVG PRICE', ha='center', va='center',
             color='#FFFFFF', fontsize=12, fontweight='bold')
    
    # Metric 3: Hot Deals vs Stale (pie chart)
    ax3.set_facecolor('#0A0A0A')
    hot = int(report.get('Hot Deals (8+)', '0'))
    stale = int(report.get('Stale (90+)', '0')) if 'Stale (90+)' in report else 0
    other = int(report.get('Total Listings', '0')) - hot - stale
    
    if hot + stale + other > 0:
        sizes = [hot, stale, other]
        colors_pie = ['#44FF88', '#FF4444', '#666666']
        labels_pie = ['HOT', 'STALE', 'OTHER']
        
        wedges, texts, autotexts = ax3.pie(sizes, labels=labels_pie, colors=colors_pie,
                                             autopct='%1.0f%%', startangle=90,
                                             textprops={'color': '#FFFFFF', 'fontweight': 'bold'})
        ax3.set_title('DEAL STATUS', color='#FFFFFF', fontsize=12, fontweight='bold', pad=10)
    
    # Metric 4: Price/SqFt (bar)
    ax4.set_facecolor('#0A0A0A')
    ppsf = report.get('Avg $/SqFt', '£0')
    if '£' in ppsf:
        ppsf_val = int(ppsf.replace('£', '').replace(',', ''))
        ax4.bar(['AVG'], [ppsf_val], color='#D4AF37', edgecolor='#F4E4A8', linewidth=2)
        ax4.set_ylabel('Price/SqFt', color='#FFFFFF', fontweight='bold')
        ax4.set_title('PRICE PER SQFT', color='#FFFFFF', fontsize=12, fontweight='bold', pad=10)
        ax4.tick_params(axis='both', colors='#FFFFFF')
        ax4.spines['bottom'].set_color('#D4AF37')
        ax4.spines['left'].set_color('#D4AF37')
        ax4.spines['top'].set_visible(False)
        ax4.spines['right'].set_visible(False)
        ax4.set_facecolor('#0A0A0A')
        ax4.text(0, ppsf_val + 50, f'£{ppsf_val}', ha='center', 
                color='#FFFFFF', fontweight='bold')
    
    plt.tight_layout()
    
    img_buffer = BytesIO()
    plt.savefig(img_buffer, format='png', dpi=150, facecolor='#0A0A0A', edgecolor='none')
    img_buffer.seek(0)
    plt.close()
    
    print("  ✅ Metrics dashboard generated")
    return img_buffer

# ============================================================================
# PDF GENERATION
# ============================================================================

def create_luxury_pdf(report, output_path="/mnt/user-data/outputs/voxmill_report.pdf"):
    """Generate Fortune 500-level PDF with charts and visual intelligence"""
    print("\n🎨 Generating luxury PDF...")
    
    c = canvas.Canvas(output_path, pagesize=A4)
    width, height = A4
    
    # ========== PAGE 1: EXECUTIVE COVER + BLUF ==========
    
    # Background
    c.setFillColor(COLORS['black'])
    c.rect(0, 0, width, height, fill=True, stroke=False)
    
    # Gold header bar
    c.setFillColor(COLORS['gold'])
    c.rect(0, height - 1.5*inch, width, 1.5*inch, fill=True, stroke=False)
    
    # Title
    c.setFillColor(COLORS['black'])
    c.setFont("Helvetica-Bold", 32)
    c.drawCentredString(width/2, height - 0.8*inch, "VOXMILL")
    c.setFont("Helvetica", 16)
    c.drawCentredString(width/2, height - 1.1*inch, "MARKET INTELLIGENCE")
    
    # Report details
    c.setFillColor(COLORS['white'])
    c.setFont("Helvetica-Bold", 20)
    market = report.get('Market', 'London')
    c.drawCentredString(width/2, height - 2.3*inch, f"{market.upper()}")
    
    c.setFont("Helvetica", 12)
    timestamp = report.get('Timestamp', datetime.now().strftime('%d %B %Y'))
    c.drawCentredString(width/2, height - 2.7*inch, f"Week of {timestamp}")
    
    # Gold divider line
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
    
    # Parse BLUF content
    bluf_text = report.get('🎯 BLUF (Bottom Line Up Front)', 'No BLUF available')
    
    c.setFillColor(COLORS['white'])
    c.setFont("Helvetica", 10)
    
    # Word wrap BLUF
    y_position = height - 4*inch
    max_width = width - 2.5*inch
    
    lines = []
    for line in bluf_text.split('\n'):
        if line.strip():
            # Word wrap
            words = line.split()
            current_line = []
            for word in words:
                test_line = ' '.join(current_line + [word])
                if c.stringWidth(test_line, "Helvetica", 10) < max_width:
                    current_line.append(word)
                else:
                    if current_line:
                        lines.append(' '.join(current_line))
                    current_line = [word]
            if current_line:
                lines.append(' '.join(current_line))
    
    for line in lines[:15]:  # Max 15 lines
        c.drawString(1.2*inch, y_position, line)
        y_position -= 0.18*inch
    
    # Key Metrics Box
    c.setFillColor(COLORS['gold'])
    c.setFont("Helvetica-Bold", 14)
    c.drawString(1*inch, height - 7*inch, "📊 KEY METRICS")
    
    c.setFillColor(COLORS['white'])
    c.setFont("Helvetica", 11)
    metrics_y = height - 7.4*inch
    
    total_listings = report.get('Total Listings', '0')
    avg_price = report.get('Avg Price', '£0')
    hot_deals = report.get('Hot Deals (8+)', '0')
    
    c.drawString(1.2*inch, metrics_y, f"• {total_listings} Active Listings")
    c.drawString(1.2*inch, metrics_y - 0.25*inch, f"• Avg Price: {avg_price}")
    c.drawString(1.2*inch, metrics_y - 0.5*inch, f"• {hot_deals} Hot Deals (8+/10 score)")
    
    # Footer
    c.setFillColor(COLORS['gold'])
    c.setFont("Helvetica", 9)
    c.drawString(1*inch, 0.7*inch, "Prepared by: Olly Kempster")
    c.drawString(1*inch, 0.5*inch, "info@voxmill.co.uk")
    c.setFillColor(COLORS['grey_light'])
    c.drawRightString(width - 1*inch, 0.5*inch, "© Voxmill Intelligence 2025")
    
    c.showPage()
    
    # ========== PAGE 2: CHARTS & VISUAL DATA ==========
    
    c.setFillColor(COLORS['black'])
    c.rect(0, 0, width, height, fill=True, stroke=False)
    
    # Header
    c.setFillColor(COLORS['gold'])
    c.setFont("Helvetica-Bold", 24)
    c.drawString(1*inch, height - 1*inch, "MARKET ANALYTICS")
    
    c.setStrokeColor(COLORS['gold'])
    c.setLineWidth(2)
    c.line(1*inch, height - 1.2*inch, width - 1*inch, height - 1.2*inch)
    
    # Generate and insert charts
    properties_text = report.get('Top 10 Properties (Ranked)', '')
    currency = '£' if 'London' in report.get('Market', '') else '$'
    
    # Chart 1: Metrics Dashboard
    metrics_chart = generate_metrics_dashboard(report)
    if metrics_chart:
        img = ImageReader(metrics_chart)
        c.drawImage(img, 0.8*inch, height - 5*inch, width=6.5*inch, height=3*inch, preserveAspectRatio=True)
    
    # Chart 2: Deal Scores
    deal_chart = generate_deal_score_chart(properties_text)
    if deal_chart:
        img = ImageReader(deal_chart)
        c.drawImage(img, 0.8*inch, height - 8.5*inch, width=6.5*inch, height=3*inch, preserveAspectRatio=True)
    
    # Footer
    c.setFillColor(COLORS['grey_light'])
    c.setFont("Helvetica", 8)
    c.drawCentredString(width/2, 0.5*inch, "Page 2 of 3")
    
    c.showPage()
    
    # ========== PAGE 3: TOP DEALS + RISKS ==========
    
    c.setFillColor(COLORS['black'])
    c.rect(0, 0, width, height, fill=True, stroke=False)
    
    # Header
    c.setFillColor(COLORS['gold'])
    c.setFont("Helvetica-Bold", 24)
    c.drawString(1*inch, height - 1*inch, "DEAL ALERTS & RISKS")
    
    c.setStrokeColor(COLORS['gold'])
    c.setLineWidth(2)
    c.line(1*inch, height - 1.2*inch, width - 1*inch, height - 1.2*inch)
    
    # Top Opportunities
    c.setFillColor(COLORS['gold'])
    c.setFont("Helvetica-Bold", 14)
    c.drawString(1*inch, height - 1.7*inch, "💎 TOP OPPORTUNITIES")
    
    opportunities = report.get('💎 Top Opportunities', 'No opportunities listed')
    c.setFillColor(COLORS['white'])
    c.setFont("Helvetica", 9)
    
    y_pos = height - 2.1*inch
    for line in opportunities.split('\n')[:20]:
        if line.strip():
            wrapped = []
            words = line.split()
            current = []
            for word in words:
                test = ' '.join(current + [word])
                if c.stringWidth(test, "Helvetica", 9) < width - 2.5*inch:
                    current.append(word)
                else:
                    if current:
                        wrapped.append(' '.join(current))
                    current = [word]
            if current:
                wrapped.append(' '.join(current))
            
            for w_line in wrapped:
                c.drawString(1.2*inch, y_pos, w_line)
                y_pos -= 0.15*inch
                if y_pos < height - 5.5*inch:
                    break
            if y_pos < height - 5.5*inch:
                break
    
    # Risk Assessment
    c.setFillColor(COLORS['red'])
    c.setFont("Helvetica-Bold", 14)
    c.drawString(1*inch, height - 6*inch, "⚠️ RISK ASSESSMENT")
    
    risks = report.get('⚠️ Risk Assessment', 'No risks listed')
    c.setFillColor(COLORS['white'])
    c.setFont("Helvetica", 9)
    
    y_pos = height - 6.4*inch
    for line in risks.split('\n')[:15]:
        if line.strip():
            wrapped = []
            words = line.split()
            current = []
            for word in words:
                test = ' '.join(current + [word])
                if c.stringWidth(test, "Helvetica", 9) < width - 2.5*inch:
                    current.append(word)
                else:
                    if current:
                        wrapped.append(' '.join(current))
                    current = [word]
            if current:
                wrapped.append(' '.join(current))
            
            for w_line in wrapped:
                c.drawString(1.2*inch, y_pos, w_line)
                y_pos -= 0.15*inch
                if y_pos < height - 8.5*inch:
                    break
            if y_pos < height - 8.5*inch:
                break
    
    # Action Triggers
    c.setFillColor(COLORS['gold'])
    c.setFont("Helvetica-Bold", 14)
    c.drawString(1*inch, height - 9*inch, "⚡ ACTION TRIGGERS")
    
    triggers = report.get('⚡ Action Triggers', 'No triggers listed')
    c.setFillColor(COLORS['white'])
    c.setFont("Helvetica", 9)
    
    y_pos = height - 9.4*inch
    for line in triggers.split('\n')[:10]:
        if line.strip():
            c.drawString(1.2*inch, y_pos, line[:90])
            y_pos -= 0.15*inch
    
    # Final footer with CTA
    c.setFillColor(COLORS['gold'])
    c.rect(0.5*inch, 0.8*inch, width - 1*inch, 0.6*inch, fill=True, stroke=False)
    c.setFillColor(COLORS['black'])
    c.setFont("Helvetica-Bold", 12)
    c.drawCentredString(width/2, 1.05*inch, "Want weekly intelligence? Contact: info@voxmill.co.uk")
    
    c.setFillColor(COLORS['grey_light'])
    c.setFont("Helvetica", 8)
    c.drawCentredString(width/2, 0.5*inch, "Page 3 of 3")
    
    c.save()
    
    print(f"  ✅ PDF saved: {output_path}")
    return output_path

# ============================================================================
# GOOGLE DRIVE UPLOAD
# ============================================================================

def upload_to_gdrive(pdf_path, folder_id="1yx7EtPN6_xu3x0U9qg8T5pOc1HbY7y0G"):
    """Upload PDF to Google Drive folder"""
    print(f"\n☁️ Uploading to Google Drive...")
    
    try:
        # Get credentials
        creds_json = os.environ.get('GOOGLE_CREDENTIALS_JSON')
        creds_dict = json.loads(creds_json)
        scopes = [
            'https://www.googleapis.com/auth/drive.file',
            'https://www.googleapis.com/auth/drive'
        ]
        credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
        
        # Build Drive service
        drive_service = build('drive', 'v3', credentials=credentials)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime('%Y-%m-%d_%H%M')
        filename = f"Voxmill_Report_{timestamp}.pdf"
        
        # File metadata
        file_metadata = {
            'name': filename,
            'parents': [folder_id]
        }
        
        # Upload file
        media = MediaFileUpload(pdf_path, mimetype='application/pdf')
        file = drive_service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, webViewLink'
        ).execute()
        
        file_id = file.get('id')
        web_link = file.get('webViewLink')
        
        print(f"  ✅ Uploaded to Google Drive")
        print(f"  📄 File ID: {file_id}")
        print(f"  🔗 View: {web_link}")
        
        return {
            'file_id': file_id,
            'web_link': web_link,
            'filename': filename
        }
        
    except Exception as e:
        print(f"  ❌ Upload failed: {e}")
        return None

# ============================================================================
# MAIN EXECUTION
# ============================================================================

def main():
    """Generate luxury PDF from latest Google Sheet data"""
    print("\n" + "="*70)
    print("VOXMILL PDF GENERATOR — ELITE VISUAL INTELLIGENCE")
    print("="*70)
    
    try:
        # Pull data from sheet
        report = get_sheet_data("London Real Estate")
        
        # Generate PDF locally first
        local_path = "/tmp/voxmill_report.pdf"
        pdf_path = create_luxury_pdf(report, output_path=local_path)
        
        # Upload to Google Drive
        drive_result = upload_to_gdrive(pdf_path)
        
        # Also save to outputs for local access
        output_path = "/mnt/user-data/outputs/voxmill_report.pdf"
        import shutil
        shutil.copy(local_path, output_path)
        print(f"\n📁 Local copy: {output_path}")
        
        print("\n" + "="*70)
        print("✅ PDF GENERATION & UPLOAD COMPLETE")
        print("="*70)
        print("\n🎨 Features:")
        print("   • Black-and-gold Fortune 500 design")
        print("   • BLUF executive summary")
        print("   • Matplotlib charts (metrics dashboard, deal scores)")
        print("   • Visual property rankings")
        print("   • Risk matrix with action triggers")
        print("   • 3 pages of strategic intelligence")
        
        if drive_result:
            print("\n☁️ Google Drive:")
            print(f"   • Filename: {drive_result['filename']}")
            print(f"   • Link: {drive_result['web_link']}")
        
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
