"""
VOXMILL WHATSAPP HANDLER
========================
Handles incoming WhatsApp messages with V3 predictive intelligence + welcome messages
"""

import os
import logging
import httpx
from datetime import datetime, timezone
from twilio.rest import Client
from app.dataset_loader import load_dataset
from app.llm import classify_and_respond
from app.utils import format_analyst_response, log_interaction

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Twilio configuration
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER")

twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN) if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN else None


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


async def handle_whatsapp_message(sender: str, message_text: str):
    """
    Main message handler with V3 predictive intelligence + edge case handling + PDF delivery + welcome messages
    """
    try:
        logger.info(f"Processing message from {sender}: {message_text}")
        
        # ========================================
        # EDGE CASE HANDLING
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
        
        # Case 3: Only emojis/symbols (no actual text)
        import re
        text_only = re.sub(r'[^\w\s]', '', message_text)
        if len(text_only.strip()) < 2:
            await send_twilio_message(
                sender,
                "I specialise in market intelligence analysis. What would you like to explore? (Market overview, opportunities, competitive landscape, scenario modelling)"
            )
            return
        
        # Case 4: Detect common typos and normalize
        message_normalized = normalize_query(message_text)
        
        # ========================================
        # LOAD CLIENT PROFILE & CHECK FIRST TIME
        # ========================================
        
        from app.client_manager import get_client_profile, update_client_history
        client_profile = get_client_profile(sender)
        
        # Check if first-time user (total_queries == 0)
        is_first_time = client_profile.get('total_queries', 0) == 0
        
        if is_first_time:
            # Send welcome message for first-time users
            await send_first_time_welcome(sender, client_profile)
        
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
        # NORMAL PROCESSING CONTINUES
        # ========================================
        
        # Get preferred region from profile
        preferred_region = client_profile.get('preferences', {}).get('preferred_regions', ['Mayfair'])[0]
        
        # Load dataset for preferred region
        dataset = load_dataset(area=preferred_region)
        
        # Detect trends
        try:
            from app.intelligence.trend_detector import detect_market_trends
            trends = detect_market_trends(area=preferred_region, lookback_days=14)
            if trends:
                dataset['detected_trends'] = trends
        except (ImportError, Exception) as e:
            logger.debug(f"Trend detection unavailable: {str(e)}")
        
        # ADD LAYER 1: Agent Profiling
        try:
            from app.intelligence.agent_profiler import get_agent_profiles
            agent_profiles = get_agent_profiles(area=preferred_region)
            if agent_profiles:
                dataset['agent_profiles'] = agent_profiles
        except (ImportError, Exception) as e:
            logger.debug(f"Agent profiler unavailable: {str(e)}")
        
        # ADD LAYER 2: Micro-Market Segmentation
        try:
            from app.intelligence.micromarket_segmenter import segment_micromarkets
            micromarkets = segment_micromarkets(dataset.get('properties', []), preferred_region)
            if micromarkets and 'error' not in micromarkets:
                dataset['micromarkets'] = micromarkets
        except (ImportError, Exception) as e:
            logger.debug(f"Micromarket segmenter unavailable: {str(e)}")
        
        # ADD LAYER 3: Liquidity Velocity
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
        
        # ADD LAYER 4: Cascade Prediction
        try:
            from app.intelligence.cascade_predictor import build_agent_network, predict_cascade
            
            # Check if user query implies scenario modelling
            scenario_keywords = ['what if', 'simulate', 'scenario', 'predict', 'cascade']
            is_scenario_query = any(keyword in message_normalized.lower() for keyword in scenario_keywords)
            
            if is_scenario_query:
                # Build network if not cached
                network = build_agent_network(area=preferred_region, lookback_days=90)
                
                if not network.get('error'):
                    # Try to extract scenario from query
                    import re
                    
                    # Look for patterns like "Knight Frank drops 10%"
                    agent_pattern = r'(Knight Frank|Savills|Hamptons|Chestertons|Strutt & Parker|[\w\s&]+?)\s+(?:drops?|reduces?|lowers?|cuts?|increases?|raises?)(?:\s+by)?\s+(\d+\.?\d*)%'
                    match = re.search(agent_pattern, message_normalized, re.IGNORECASE)
                    
                    if match:
                        initiating_agent = match.group(1).strip()
                        magnitude = float(match.group(2))
                        
                        # Determine if increase or decrease
                        if any(word in message_normalized.lower() for word in ['drop', 'reduce', 'lower', 'cut']):
                            magnitude = -magnitude
                        
                        # Run cascade prediction
                        cascade = predict_cascade(network, initiating_agent, magnitude)
                        if not cascade.get('error'):
                            dataset['cascade_prediction'] = cascade
                            logger.info(f"Cascade predicted: {initiating_agent} {magnitude:+.1f}% -> {cascade['total_affected_agents']} agents affected")
        except (ImportError, Exception) as e:
            logger.debug(f"Cascade predictor unavailable: {str(e)}")
        
        # Classify and respond (use normalized message)
        category, response_text, response_metadata = await classify_and_respond(
            message_normalized,
            dataset,
            client_profile=client_profile
        )
        
        # Format response
        formatted_response = format_analyst_response(response_text, category)
        
        # Send via Twilio (with smart chunking)
        await send_twilio_message(sender, formatted_response)
        
        # Log interaction and update client history (use original message)
        log_interaction(sender, message_text, category, formatted_response)
        update_client_history(sender, message_text, category, preferred_region)
        
        logger.info(f"Message processed: {category} | Confidence: {response_metadata.get('confidence_level')} | Urgency: {response_metadata.get('recommendation_urgency')}")
        
    except Exception as e:
        logger.error(f"Error handling message: {str(e)}", exc_info=True)
        error_msg = "System encountered an error processing your request. Please try rephrasing your query or contact support if this persists."
        await send_twilio_message(sender, error_msg)


async def send_pdf_report(sender: str, area: str):
    """
    Generate and send PDF report link to client
    """
    try:
        logger.info(f"PDF report requested by {sender} for {area}")
        
        # Check if PDF storage is configured
        try:
            from app.pdf_storage import (Import is correct, but ensure pdf_storage.py exists - FILE 6)
            storage_available = True
        except ImportError:
            storage_available = False
        
        if not storage_available:
            logger.warning("PDF storage module not configured")
            message = (
                "PDF delivery is being configured for your account. "
                "In the meantime, I can provide comprehensive intelligence via text. "
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
            
            message = (
                "📊 EXECUTIVE INTELLIGENCE BRIEFING\n"
                "————————————————————————————————————————\n\n"
                f"{area} Market Analysis\n"
                f"Generated: {date_str}\n\n"
                f"View your report:\n{pdf_url}\n\n"
                "📌 Link valid for 7 days\n"
                "📄 14-page institutional-grade analysis"
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
