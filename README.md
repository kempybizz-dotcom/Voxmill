VOXMILL MARKET INTELLIGENCE — DEPLOYMENT GUIDE
===============================================

This package contains everything you need to run automated market intelligence reports
for London real estate agencies.

📦 WHAT'S INCLUDED
==================

1. voxmill_elite_v2.py — Data collection script (pulls from Zoopla, RapidAPI, Outscraper)
2. voxmill_pdf_generator.py — PDF generation with charts (uploads to Google Drive)
3. requirements.txt — Python dependencies
4. This README


🎯 WHAT IT DOES
===============

STEP 1: Data Collection (voxmill_elite_v2.py)
----------------------------------------------
• Collects Miami real estate data (US market)
• Collects London real estate data (Zoopla via RapidAPI)
• Collects London luxury car rental data (Google Places)
• Generates AI insights (BLUF, opportunities, risks, action triggers)
• Writes to Google Sheets with Fortune 500 formatting

STEP 2: PDF Generation (voxmill_pdf_generator.py)
--------------------------------------------------
• Pulls latest data from Google Sheet
• Generates 3-page luxury PDF with:
  - Black-and-gold Fortune 500 design
  - BLUF executive summary
  - Matplotlib charts (metrics dashboard, deal scores, pie charts)
  - Top opportunities and risk assessment
  - Action triggers (IF-THEN framework)
• Uploads to Google Drive (folder: 1yx7EtPN6_xu3x0U9qg8T5pOc1HbY7y0G)
• Saves local copy to /mnt/user-data/outputs/


🚀 DEPLOYMENT INSTRUCTIONS
==========================

OPTION A: RENDER.COM (AUTOMATED WEEKLY CRON)
---------------------------------------------

1. CREATE NEW WEB SERVICE
   • Go to Render.com
   • Click "New +" → "Web Service"
   • Connect your GitHub repo (or upload these files)

2. SET ENVIRONMENT VARIABLES
   In Render dashboard, go to Environment and add:

   RAPIDAPI_KEY=1440de56aamsh945d6c41f441399p1af6adjsne2d964758775
   OUTSCRAPER_API_KEY=[your Outscraper key]
   OPENAI_API_KEY=[your OpenAI key]
   GOOGLE_SHEET_ID=[your Google Sheet ID]
   GOOGLE_CREDENTIALS_JSON=[paste your service account JSON here]

   To get GOOGLE_CREDENTIALS_JSON:
   • Go to Google Cloud Console
   • Create service account
   • Download JSON key
   • Share your Google Sheet with the service account email
   • Copy entire JSON contents into this variable

3. SET BUILD COMMAND
   pip install -r requirements.txt

4. SET START COMMAND
   python voxmill_elite_v2.py && python voxmill_pdf_generator.py

5. ADD CRON JOB (OPTIONAL)
   In Render dashboard:
   • Go to "Cron Jobs"
   • Schedule: "0 9 * * 1" (Every Monday at 9am)
   • Command: python voxmill_elite_v2.py && python voxmill_pdf_generator.py


OPTION B: LOCAL TESTING (YOUR COMPUTER)
----------------------------------------

1. INSTALL PYTHON 3.12+
   Download from python.org

2. INSTALL DEPENDENCIES
   Open terminal and run:
   pip install -r requirements.txt

3. SET ENVIRONMENT VARIABLES
   Create a file called .env and add:

   RAPIDAPI_KEY=1440de56aamsh945d6c41f441399p1af6adjsne2d964758775
   OUTSCRAPER_API_KEY=[your key]
   OPENAI_API_KEY=[your key]
   GOOGLE_SHEET_ID=[your sheet ID]
   GOOGLE_CREDENTIALS_JSON=[your service account JSON]

   Then in terminal:
   export $(cat .env | xargs)

4. RUN SCRIPTS
   # Collect data
   python voxmill_elite_v2.py

   # Generate PDF
   python voxmill_pdf_generator.py


📊 GOOGLE SHEETS SETUP
======================

The script will create 3 sheets automatically:
1. Miami Real Estate
2. London Real Estate  
3. London Luxury Car Rental

Each sheet has:
• Black header row with white text
• Color-coded columns (green for hot deals, yellow for stale, gold for ultra-luxury)
• Frozen header row
• Professional column widths


☁️ GOOGLE DRIVE SETUP
=====================

PDFs are automatically uploaded to your Google Drive folder:
Folder ID: 1yx7EtPN6_xu3x0U9qg8T5pOc1HbY7y0G

Filename format: Voxmill_Report_2025-10-27_1430.pdf (timestamped)

Make sure your service account has access to this folder:
1. Open the folder in Google Drive
2. Click "Share"
3. Add your service account email (ends with @*.iam.gserviceaccount.com)
4. Give "Editor" permission


📧 ZAPIER EMAIL AUTOMATION (OPTIONAL)
=====================================

OPTION 1: Watch Google Drive
-----------------------------
Trigger: New file in Google Drive folder
Action: Send email with PDF attachment

Steps:
1. Zapier → Create Zap
2. Trigger: Google Drive → New File in Folder
3. Select folder: 1yx7EtPN6_xu3x0U9qg8T5pOc1HbY7y0G
4. Action: Gmail/Outlook → Send Email
5. Attach file from trigger
6. Subject: "Voxmill Market Intelligence — [Date]"
7. Body: "Your weekly London market report is attached."


OPTION 2: Watch Google Sheet
-----------------------------
Trigger: New row in Google Sheet
Action: Get file from Drive → Send email

Steps:
1. Zapier → Create Zap
2. Trigger: Google Sheets → New Row
3. Select sheet: London Real Estate
4. Action: Google Drive → Find File (by timestamp)
5. Action: Gmail/Outlook → Send Email
6. Attach file from step 4


🎯 YOUR OUTREACH WORKFLOW
=========================

NOW THAT YOU HAVE THE SYSTEM RUNNING:

1. RUN THE SCRIPT (Monday morning)
   • Generates fresh London real estate data
   • Creates luxury PDF with charts
   • Uploads to Google Drive

2. MAKE YOUR CALLS (Monday-Wednesday)
   Call script:
   
   "Hi, this is Olly from Voxmill Market Intelligence. We track luxury property
   pricing across London for boutique agencies.
   
   I just ran this week's report on [Mayfair/Knightsbridge] and found [X] properties
   trading below market average on price-per-square-foot. A couple have been sitting
   for 90+ days.
   
   I thought [Director Name] would want to see this before the weekend — it's a
   1-page intelligence brief with charts and deal scores. Can I send it over to
   their email?"

3. SEND THE PDF (immediately after call)
   • Forward the Google Drive link
   • Or attach the PDF from /mnt/user-data/outputs/

4. FOLLOW UP (Thursday)
   Email subject: "Following up — [Their Area] market report"
   
   "Hi [Director Name],
   
   Wanted to follow up on the market intelligence report I sent Monday.
   
   Quick question: Would a weekly version of this focused on your specific
   focus areas be useful? I can customize it to track properties in your
   target price range.
   
   If you'd like to discuss, I'm available for a 10-minute call.
   
   — Olly"

5. CLOSE THE DEAL (Friday)
   If they respond positively:
   
   "Perfect. Here's what I propose:
   
   £700/month for weekly London market intelligence reports:
   • Your specific neighborhoods (Mayfair, Knightsbridge, etc.)
   • Hot deals scored 8+/10 (underpriced properties)
   • Stale listings (90+ days — negotiation leverage)
   • Risk alerts and action triggers
   • Delivered every Monday at 9am
   
   First report is free. If you like it, we invoice monthly.
   
   Sound good?"


💰 PRICING GUIDE
================

YOUR OFFER:
£700-1,000/month per client for weekly reports

COST PER REPORT:
• Outscraper: ~$0.10
• OpenAI: ~$0.01
• RapidAPI: Included in subscription
• Total: ~£0.10 per report

ROI:
£700/month client = 7,000x cost
£1,000/month client = 10,000x cost

SCALE TARGETS:
• 5 clients = £3,500-5,000 MRR
• 10 clients = £7,000-10,000 MRR
• 20 clients = £14,000-20,000 MRR

All automated. Zero manual work per client.


🔧 TROUBLESHOOTING
==================

"Script fails with 'No module named X'"
→ Run: pip install -r requirements.txt --break-system-packages

"Can't connect to Google Sheets"
→ Check GOOGLE_CREDENTIALS_JSON is set correctly
→ Make sure service account has access to the sheet

"Zoopla returns no data"
→ Check RAPIDAPI_KEY is correct
→ Check RapidAPI subscription includes Zoopla API

"PDF has no charts"
→ Make sure matplotlib and reportlab are installed
→ Check Google Sheet has data in it

"Upload to Google Drive fails"
→ Share Drive folder with service account email
→ Give "Editor" permission


📞 SUPPORT
==========

If you get stuck:
1. Check error messages in terminal/Render logs
2. Verify all environment variables are set
3. Test each script separately (data collection, then PDF generation)


🎉 YOU'RE READY
===============

You now have a fully automated market intelligence system that:
✅ Collects real market data weekly
✅ Generates Fortune 500-level PDFs with charts
✅ Uploads to Google Drive automatically
✅ Can email to clients via Zapier

YOUR ONLY JOB:
Make calls. Send PDFs. Close deals.

Target: 1 client at £700-1,000/month in 30 days.

STOP BUILDING. START SELLING.

— Voxmill Operations Architect
