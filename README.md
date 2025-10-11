# VOXMILL MARKET INTELLIGENCE ENGINE

Automated real estate market intelligence reports for US markets.

## Architecture

```
Outscraper (Zillow/Google Maps) → Flask API → OpenAI Analysis → JSON Response
```

## Endpoints

- **`GET /`** - Web UI for testing
- **`POST /generate-report`** - Generate market report
- **`GET /health`** - Health check

## Environment Variables

Set these in Render dashboard:

```
OUTSCRAPER_API_KEY=your_key_here
OPENAI_API_KEY=your_key_here
```

## Deployment

1. Push code to GitHub
2. Render auto-deploys from `main` branch
3. Access at: `https://voxmill-report-engine.onrender.com`

## Testing

### Via Web UI
Navigate to your Render URL and use the form.

### Via cURL
```bash
curl -X POST https://your-render-url.onrender.com/generate-report \
  -H "Content-Type: application/json" \
  -d '{"city": "Miami", "state": "FL", "property_type": "luxury"}'
```

## Cost Per Report

- Outscraper: ~$0.10 per 10 listings
- OpenAI: ~$0.01 per analysis
- **Total: ~$0.11 per report**

At £700/month per client = 6,363x ROI per client.

## Next Steps

1. Test with Miami, Austin, Denver
2. Add PDF generation (Google Slides API)
3. Add email delivery (Gmail API)
4. Set up weekly cron job

---

**Status**: MVP - JSON output only (PDF coming next) 
