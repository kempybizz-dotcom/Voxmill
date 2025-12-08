"""
VOXMILL WHATSAPP HANDLER
========================
"""

import os
import logging
import httpx
from datetime import datetime, timezone, timedelta  # ← Make sure timedelta is here
from twilio.rest import Client
from app.dataset_loader import load_dataset
from app.llm import classify_and_respond
from app.utils import format_analyst_response, log_interaction
from app.whatsapp_self_service import handle_whatsapp_preference_message

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Twilio configuration
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER")

twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN) if TWILIO_ACCOUNT_SID


async def send_first_time_welcome(sender: str, client_profile: dict):
    """
    Send welcome message to first-time users
    """
    try:
        tier = client_profile.get('tier', 'tier_1')
        name = client_profile.get('name', 'there')
        
        # Split name to get first name only
        first_name = name.split()[0] if name != 'there' else 'there'
        
        # Tier-specific welcome messages
        welcome_messages = {
            "tier_1": f"""Welcome to Voxmill Intelligence, {first_name}.

Your Tier 1 access is now active.

You have access to:
- Real-time market overview
- Competitive intelligence
- Opportunity identification
- Price corridor analysis

Ask me anything about luxury markets. Try:
- "Market overview"
- "Top opportunities"
- "Competitive landscape"

Available 24/7 at this number.""",

            "tier_2": f"""Welcome to Voxmill Intelligence, {first_name}.

Your Tier 2 Analyst Desk is now active.

You have full access to:
- Real-time market intelligence
- Competitive dynamics analysis
- Trend detection (14-day windows)
- Strategic recommendations
- Liquidity velocity tracking
- Up to 10 analyses per day

Your intelligence is personalized to your preferences and will learn from our conversations.

Ask me anything. Try:
- "What's the market outlook?"
- "Analyze competitive positioning"
- "Show me liquidity trends"

Available 24/7.""",

            "tier_3": f"""Welcome to Voxmill Intelligence, {first_name}.

Your Tier 3 Strategic Partner access is now active.

You have unlimited access to our complete intelligence suite:

REAL-TIME ANALYSIS:
- Market overview & trends
- Competitive landscape
- Opportunity identification

PREDICTIVE INTELLIGENCE:
- Agent behavioral profiling (85-91% confidence)
- Multi-wave cascade forecasting
- Liquidity velocity tracking
- Micromarket segmentation

SCENARIO MODELING:
- "What if Knight Frank drops 10%?"
- Strategic response recommendations
- Risk/opportunity mapping

No message limits. Full institutional-grade intelligence.

Ask me anything, anytime. Examples:
- "Strategic outlook for Mayfair"
- "What if Savills raises prices 8%?"
- "Analyze liquidity velocity"
- "Predict cascade effects"

Your dedicated intelligence partner, available 24/7."""
        }
        
        message = welcome_messages.get(tier, welcome_messages["tier_1"])
        
        await send_twilio_message(sender, message)
        logger.info(f"Welcome message sent to {sender} (Tier: {tier})")
        
        # Small delay before processing their first query
        import asyncio
        await asyncio.sleep(1.5)
        
    except Exception as e:
        logger.error(f"Error sending welcome message: {str(e)}", exc_info=True)


# Add to whatsapp.py at the TOP of handle_whatsapp_message()

async def handle_whatsapp_message(sender: str, message_text: str):
    """
    Main message handler with V3 predictive intelligence + edge case handling + 
    PDF delivery + welcome messages + rate limiting + spam protection
    """
    
    try:
        logger.info(f"Processing message from {sender}: {message_text}")
        
        # ========================================
        # EDGE CASE HANDLING - FIRST LINE OF DEFENSE
        # ========================================
        
        # Case 1: Empty message
        if not message_text or not message_text.strip():
            await send_twilio_message(
                sender, 
                "I didn't receive a message. Please send your market intelligence query."
            )
            return
        
        # Case 2: Message too short (likely accidental)
        if len(message_text.strip()) < 2:
            await send_twilio_message(
                sender,
                "I didn't catch that. Ask me about market analysis, competitive intelligence, or strategic forecasting."
            )
            return
        
        # Case 3: Meaningless messages (dots, question marks, etc.)
        if message_text.strip() in ['...', '???', '!!!', '?', '.', '!', '..', '??', '!!']:
            await send_twilio_message(
                sender,
                "I didn't catch that. Ask me about market analysis, competitive intelligence, or strategic forecasting."
            )
            return
        
        # Case 4: Only emojis/symbols (no actual text)
        import re
        text_only = re.sub(r'[^\w\s]', '', message_text)
        if len(text_only.strip()) < 2:
            await send_twilio_message(
                sender,
                "I specialise in market intelligence analysis. What would you like to explore? (Market overview, opportunities, competitive landscape, scenario modelling)"
            )
            return
        
        # ========================================
        # LOAD CLIENT PROFILE
        # ========================================
        
        from app.client_manager import get_client_profile, update_client_history
        client_profile = get_client_profile(sender)
        
        # ========================================
        # RATE LIMITING - PREVENT COST EXPLOSION
        # ========================================
        
        query_history = client_profile.get('query_history', [])
        
        # SPAM PROTECTION: Minimum 2 seconds between messages
        if query_history:
            last_query_time = query_history[-1].get('timestamp')
            if last_query_time:
                seconds_since_last = (datetime.now(timezone.utc) - last_query_time).total_seconds()
                
                if seconds_since_last < 2:
                    # Too fast - silently ignore (likely accidental double-tap)
                    logger.warning(f"Spam protection triggered for {sender} ({seconds_since_last:.1f}s since last)")
                    return
        
        # RATE LIMITING: Queries per hour by tier
        one_hour_ago = datetime.now(timezone.utc) - timedelta(hours=1)
        recent_queries = [
            q for q in query_history 
            if q.get('timestamp') and q['timestamp'] > one_hour_ago
        ]
        
        tier = client_profile.get('tier', 'tier_1')
        limits = {
            'tier_1': 10,   # 10/hour
            'tier_2': 50,   # 50/hour
            'tier_3': 200   # 200/hour (not truly unlimited to prevent abuse)
        }
        
        max_queries = limits.get(tier, 10)
        
        if len(recent_queries) >= max_queries:
            # Calculate time until reset
            oldest_query = min(q['timestamp'] for q in recent_queries)
            time_until_reset = (oldest_query + timedelta(hours=1) - datetime.now(timezone.utc))
            minutes_until_reset = int(time_until_reset.total_seconds() / 60)
            
            rate_limit_msg = f"""⚠️ RATE LIMIT REACHED

Your {tier.replace('_', ' ').title()} plan allows {max_queries} queries per hour.

Reset in: {minutes_until_reset} minutes

Need more queries? Upgrade or contact:
📧 ollys@voxmill.uk"""
            
            await send_twilio_message(sender, rate_limit_msg)
            logger.warning(f"Rate limit hit for {sender} ({tier}): {len(recent_queries)}/{max_queries}")
            return
        
        # ========================================
        # MESSAGE LENGTH LIMIT - PREVENT COST EXPLOSION
        # ========================================
        
        MAX_MESSAGE_LENGTH = 500
        
        if len(message_text) > MAX_MESSAGE_LENGTH:
            await send_twilio_message(
                sender,
                f"Message too long ({len(message_text)} characters). "
                f"Please keep queries under {MAX_MESSAGE_LENGTH} characters for optimal analysis."
            )
            logger.warning(f"Message too long from {sender}: {len(message_text)} chars")
            return
        
        # ========================================
        # PREFERENCE SELF-SERVICE
        # ========================================
        
        try:
            logger.info(f"🔍 Checking if message is preference request: {message_text[:50]}")
            pref_response = handle_whatsapp_preference_message(sender, message_text)
            
            if pref_response:
                logger.info(f"✅ Preference request detected, sending confirmation")
                await send_twilio_message(sender, pref_response)
                
                # Log the preference change
                update_client_history(sender, message_text, "preference_update", "Self-Service")
                
                logger.info(f"✅ Preference updated via WhatsApp for {sender}")
                return
            else:
                logger.info(f"❌ Not a preference request, continuing to normal analyst")
                
        except Exception as e:
            logger.error(f"❌ ERROR in preference handler: {e}", exc_info=True)
            # Continue to normal processing
        
        # ========================================
        # FIRST-TIME USER WELCOME
        # ========================================
        
        is_first_time = client_profile.get('total_queries', 0) == 0
        
        if is_first_time:
            await send_first_time_welcome(sender, client_profile)
        
        # ========================================
        # NORMALIZE QUERY (fix typos)
        # ========================================
        
        message_normalized = normalize_query(message_text)
        
        # ========================================
        # PDF REQUEST DETECTION
        # ========================================
        
        pdf_keywords = ['send pdf', 'full report', 'send report', 'pdf please', 'get report', 
                       'view report', 'send me the pdf', 'can i see the pdf', 'full briefing',
                       'executive briefing', 'complete report', 'detailed report']
        
        is_pdf_request = any(keyword in message_normalized.lower() for keyword in pdf_keywords)
        
        if is_pdf_request:
            preferred_region = client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])[0]
            await send_pdf_report(sender, preferred_region)
            return
        
        # ========================================
        # LOAD DATASET FOR ANALYSIS
        # ========================================
        
        preferred_region = client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])[0]
        dataset = load_dataset(area=preferred_region)
        
        # Check if data exists (not fallback)
        metadata = dataset.get('metadata', {})
        is_fallback = metadata.get('is_fallback', False)
        
        if is_fallback:
            # No real data for this region
            no_data_msg = f"""⚠️ DATA UNAVAILABLE

We don't currently have market intelligence for {preferred_region}.

Available regions:
- Mayfair
- Knightsbridge
- Chelsea
- Belgravia

To add {preferred_region} coverage:
📧 ollys@voxmill.uk"""
            
            await send_twilio_message(sender, no_data_msg)
            logger.warning(f"Client {sender} requested unavailable region: {preferred_region}")
            return
        
        # Check data freshness and add warning if stale
        data_timestamp = metadata.get('analysis_timestamp')
        data_freshness_warning = ""
        
        if data_timestamp:
            data_age_hours = (datetime.now(timezone.utc) - data_timestamp).total_seconds() / 3600
            
            if data_age_hours > 48:  # Data older than 2 days
                data_freshness_warning = f"\n\n━━━━━━━━━━━━━━━━━━━━\n⚠️ Data last updated {int(data_age_hours)} hours ago"
        
        # ========================================
        # ADD V3 INTELLIGENCE LAYERS
        # ========================================
        
        # Layer 1: Trend Detection
        try:
            from app.intelligence.trend_detector import detect_market_trends
            trends = detect_market_trends(area=preferred_region, lookback_days=14)
            if trends:
                dataset['detected_trends'] = trends
        except (ImportError, Exception) as e:
            logger.debug(f"Trend detection unavailable: {str(e)}")
        
        # Layer 2: Agent Profiling
        try:
            from app.intelligence.agent_profiler import get_agent_profiles
            agent_profiles = get_agent_profiles(area=preferred_region)
            if agent_profiles:
                dataset['agent_profiles'] = agent_profiles
        except (ImportError, Exception) as e:
            logger.debug(f"Agent profiler unavailable: {str(e)}")
        
        # Layer 3: Micro-Market Segmentation
        try:
            from app.intelligence.micromarket_segmenter import segment_micromarkets
            micromarkets = segment_micromarkets(dataset.get('properties', []), preferred_region)
            if micromarkets and 'error' not in micromarkets:
                dataset['micromarkets'] = micromarkets
        except (ImportError, Exception) as e:
            logger.debug(f"Micromarket segmenter unavailable: {str(e)}")
        
        # Layer 4: Liquidity Velocity
        try:
            from app.intelligence.liquidity_velocity import calculate_liquidity_velocity
            from app.dataset_loader import load_historical_snapshots
            historical = load_historical_snapshots(area=preferred_region, days=30)
            if historical:
                velocity = calculate_liquidity_velocity(dataset.get('properties', []), historical)
                if velocity and 'error' not in velocity:
                    dataset['liquidity_velocity'] = velocity
        except (ImportError, Exception) as e:
            logger.debug(f"Liquidity velocity unavailable: {str(e)}")
        
        # Layer 5: Cascade Prediction (if scenario query)
        try:
            from app.intelligence.cascade_predictor import build_agent_network, predict_cascade
            
            scenario_keywords = ['what if', 'simulate', 'scenario', 'predict', 'cascade']
            is_scenario_query = any(keyword in message_normalized.lower() for keyword in scenario_keywords)
            
            if is_scenario_query:
                network = build_agent_network(area=preferred_region, lookback_days=90)
                
                if not network.get('error'):
                    # Extract scenario from query
                    import re
                    agent_pattern = r'(Knight Frank|Savills|Hamptons|Chestertons|Strutt & Parker|[\w\s&]+?)\s+(?:drops?|reduces?|lowers?|cuts?|increases?|raises?)(?:\s+by)?\s+(\d+\.?\d*)%'
                    match = re.search(agent_pattern, message_normalized, re.IGNORECASE)
                    
                    if match:
                        initiating_agent = match.group(1).strip()
                        magnitude = float(match.group(2))
                        
                        if any(word in message_normalized.lower() for word in ['drop', 'reduce', 'lower', 'cut']):
                            magnitude = -magnitude
                        
                        cascade = predict_cascade(network, initiating_agent, magnitude)
                        if not cascade.get('error'):
                            dataset['cascade_prediction'] = cascade
                            logger.info(f"Cascade predicted: {initiating_agent} {magnitude:+.1f}% -> {cascade['total_affected_agents']} agents affected")
        except (ImportError, Exception) as e:
            logger.debug(f"Cascade predictor unavailable: {str(e)}")
        
        # ========================================
        # GPT-4 ANALYSIS
        # ========================================
        
        category, response_text, response_metadata = await classify_and_respond(
            message_normalized,
            dataset,
            client_profile=client_profile
        )
        
        # Format response
        formatted_response = format_analyst_response(response_text, category)
        
        # Add data freshness warning if needed
        formatted_response += data_freshness_warning
        
        # ========================================
        # SEND RESPONSE
        # ========================================
        
        await send_twilio_message(sender, formatted_response)
        
        # Log interaction and update client history
        log_interaction(sender, message_text, category, formatted_response)
        update_client_history(sender, message_text, category, preferred_region)
        
        logger.info(f"Message processed: {category} | Confidence: {response_metadata.get('confidence_level')} | Urgency: {response_metadata.get('recommendation_urgency')}")
        
    except Exception as e:
        logger.error(f"Error handling message: {str(e)}", exc_info=True)
        error_msg = "System encountered an error processing your request. Please try rephrasing your query or contact support if this persists."
        await send_twilio_message(sender, error_msg)

async def send_pdf_report(sender: str, area: str):
    """
    Generate and send PDF report link to client (NO EMOJIS VERSION)
    """
    try:
        logger.info(f"PDF report requested by {sender} for {area}")
        
        # Check if PDF storage is configured
        try:
            from app.pdf_storage import get_latest_pdf_for_client, upload_pdf_to_cloud
            storage_available = True
        except ImportError:
            storage_available = False
        
        if not storage_available:
            logger.warning("PDF storage module not configured")
            message = (
                "PDF delivery is being configured for your account.\n\n"
                "In the meantime, I can provide comprehensive intelligence via text.\n\n"
                "Ask me about market overview, opportunities, or strategic outlook."
            )
            await send_twilio_message(sender, message)
            return
        
        # Try to get existing PDF URL
        pdf_url = get_latest_pdf_for_client(sender, area)
        
        if not pdf_url:
            # No existing PDF found, check temp directory
            import os
            pdf_path = "/tmp/Voxmill_Executive_Intelligence_Deck.pdf"
            
            if os.path.exists(pdf_path):
                pdf_url = upload_pdf_to_cloud(pdf_path, sender, area)
            else:
                # No PDF available
                await send_twilio_message(
                    sender,
                    f"Your {area} executive briefing is being prepared.\n\n"
                    "Reports are generated daily at midnight GMT. "
                    "The latest report will be available shortly.\n\n"
                    "In the meantime, I can provide real-time intelligence. "
                    "Ask me about market overview, opportunities, or competitive landscape."
                )
                return
        
        if pdf_url:
            from datetime import datetime
            date_str = datetime.now().strftime('%B %d, %Y')
            
            # NO EMOJIS VERSION - Professional institutional format
            message = (
                "EXECUTIVE INTELLIGENCE BRIEFING\n"
                "————————————————————————————————————————\n\n"
                f"{area} Market Analysis\n"
                f"Generated: {date_str}\n\n"
                f"View your report:\n{pdf_url}\n\n"
                "Link valid for 7 days\n"
                "14-page institutional-grade analysis"
            )
            
            await send_twilio_message(sender, message)
            logger.info(f"PDF report sent successfully to {sender}")
        else:
            await send_twilio_message(
                sender,
                f"Unable to access your {area} report at this time. "
                "Our team has been notified and will resolve this shortly."
            )
            
    except Exception as e:
        logger.error(f"Error sending PDF report: {str(e)}", exc_info=True)
        await send_twilio_message(sender, "Error generating report link. Our team has been notified.")


def normalize_query(text: str) -> str:
    """
    Normalize common typos and variations
    """
    corrections = {
        'markrt': 'market',
        'overveiw': 'overview',
        'overviw': 'overview',
        'competitve': 'competitive',
        'competetive': 'competitive',
        'oppertunities': 'opportunities',
        'oportunities': 'opportunities',
        'analyise': 'analyse',
        'analize': 'analyse',
        'scenerio': 'scenario',
        'forcast': 'forecast',
        'forceast': 'forecast',
        'whats': 'what is',
        'whta': 'what',
        'teh': 'the',
        'adn': 'and',
        'hte': 'the',
        'reportt': 'report',
        'reprot': 'report'
    }
    
    normalized = text
    for typo, correct in corrections.items():
        import re
        pattern = re.compile(re.escape(typo), re.IGNORECASE)
        normalized = pattern.sub(correct, normalized)
    
    return normalized


async def send_twilio_message(recipient: str, message: str):
    """
    Send message via Twilio WhatsApp API with intelligent chunking
    """
    try:
        if not twilio_client:
            logger.error("Twilio client not initialized")
            return
        
        # ENSURE WHATSAPP PREFIX
        from_number = TWILIO_WHATSAPP_NUMBER
        if not from_number.startswith('whatsapp:'):
            from_number = f'whatsapp:{from_number}'
        
        to_number = recipient
        if not to_number.startswith('whatsapp:'):
            to_number = f'whatsapp:{to_number}'
        
        MAX_LENGTH = 1500
        
        if len(message) <= MAX_LENGTH:
            # Short message - send as-is
            twilio_client.messages.create(
                from_=from_number,
                to=to_number,
                body=message
            )
            logger.info(f"Message sent successfully to {to_number} ({len(message)} chars)")
        else:
            # Long message - intelligent splitting
            chunks = smart_split_message(message, MAX_LENGTH)
            
            for i, chunk in enumerate(chunks, 1):
                twilio_client.messages.create(
                    from_=from_number,
                    to=to_number,
                    body=chunk
                )
                
                # Small delay to maintain order
                import asyncio
                await asyncio.sleep(0.5)
            
            logger.info(f"Multi-part message sent to {to_number} ({len(chunks)} parts, {len(message)} total chars)")
                
    except Exception as e:
        logger.error(f"Error sending Twilio message: {str(e)}", exc_info=True)
        raise


def smart_split_message(message: str, max_length: int) -> list:
    """
    Split message intelligently at natural break points
    """
    if len(message) <= max_length:
        return [message]
    
    chunks = []
    remaining = message
    
    while remaining:
        if len(remaining) <= max_length:
            chunks.append(remaining.strip())
            break
        
        # Extract a chunk of max_length
        chunk = remaining[:max_length]
        
        # Find best split point (priority order)
        split_point = -1
        
        # 1. Try double line break (major section boundary)
        double_break = chunk.rfind('\n\n')
        if double_break > max_length * 0.5:
            split_point = double_break
        
        # 2. Try single line break
        if split_point == -1:
            single_break = chunk.rfind('\n')
            if single_break > max_length * 0.5:
                split_point = single_break
        
        # 3. Try sentence end
        if split_point == -1:
            sentence_end = max(
                chunk.rfind('. '),
                chunk.rfind('! '),
                chunk.rfind('? ')
            )
            if sentence_end > max_length * 0.4:
                split_point = sentence_end + 1
        
        # 4. Try bullet point
        if split_point == -1:
            bullet = chunk.rfind('\n•')
            if bullet > max_length * 0.4:
                split_point = bullet
        
        # 5. Last resort: word boundary
        if split_point == -1:
            word_break = chunk.rfind(' ')
            if word_break > max_length * 0.3:
                split_point = word_break
        
        # Absolute fallback: hard cut
        if split_point == -1:
            split_point = max_length
        
        # Add chunk
        chunks.append(remaining[:split_point].strip())
        remaining = remaining[split_point:].strip()
    
    # Post-process: If first chunk is tiny, merge with second
    if len(chunks) > 1 and len(chunks[0]) < 200:
        chunks[0] = f"{chunks[0]}\n\n{chunks[1]}"
        chunks.pop(1)
    
    return chunks
