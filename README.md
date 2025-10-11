# VOXMILL MARKET INTELLIGENCE - AUTOMATED REPORTS

Hands-off market intelligence reports for luxury real estate clients.

## Architecture

```
Render Cron Job (weekly) → Scrapes Market Data → Writes to Google Sheet → Zapier → PDF → Email
```

## How It Works

1. **Script runs automatically** every Monday at 9am
2. **Pulls real estate data** for each client's market (Outscraper)
3. **Generates AI insights** (OpenAI: RAISE/REDUCE/ROTATE)
4. **Writes to Google Sheet** (one row per report)
5. **Zapier watches sheet** → creates PDF → emails client

## Demo Client

**Miami Brokers Group** (Mike Diaz)
- Market: Miami (Pinecrest, Coral Gables, Palmetto Bay)
- Property Type: Luxury residential

## Environment Variables (Set in Render)

```
OUTSCRAPER_API_KEY=your_outscraper_key
OPENAI_API_KEY=your_openai_key
GOOGLE_SHEET_ID=1yG10YDNwZE7BGoQ1l67xg4obGtqNyYzIYRVZi4cWit4
GOOGLE_CREDENTIALS_JSON={"type":"service_account",...}
```

## Google Sheet Structure

| Timestamp | Client Name | Contact | Market | Focus Areas | Properties Analyzed | Insights | Top 5 Properties | Status |
|-----------|-------------|---------|--------|-------------|---------------------|----------|------------------|--------|

## Cost Per Report

- Outscraper: $0.10 per 10 listings
- OpenAI: $0.01 per analysis
- **Total: ~$0.11 per report**

At £700/month per client = 6,363x ROI

## Next Steps

1. ✅ Script writes to Google Sheet
2. ⏳ Set up Zapier automation (Sheet → PDF → Email)
3. ⏳ Add real clients to system
4. ⏳ Scale to 10 clients = £7,000 MRR
