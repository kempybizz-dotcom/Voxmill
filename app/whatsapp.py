import os
import logging
import httpx
from twilio.rest import Client
from app.dataset_loader import load_dataset
from app.llm import classify_and_respond
from app.utils import format_analyst_response, log_interaction

logger = logging.getLogger(__name__)

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID")
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN")
TWILIO_WHATSAPP_NUMBER = os.getenv("TWILIO_WHATSAPP_NUMBER")

twilio_client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN) if TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN else None

async def handle_whatsapp_message(sender: str, message_text: str):
    """
    Main message handler with V3 predictive intelligence
    """
    try:
        logger.info(f"Processing message from {sender}: {message_text}")
        
        # Load client profile FIRST (need preferred region)
        from app.client_manager import get_client_profile, update_client_history
        client_profile = get_client_profile(sender)
        
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
            is_scenario_query = any(keyword in message_text.lower() for keyword in scenario_keywords)
            
            if is_scenario_query:
                # Build network if not cached
                network = build_agent_network(area=preferred_region, lookback_days=90)
                
                if not network.get('error'):
                    # Try to extract scenario from query
                    # Simple pattern matching for "Agent X drops Y%"
                    import re
                    
                    # Look for patterns like "Knight Frank drops 10%" or "if Savills reduces by 8%"
                    agent_pattern = r'(Knight Frank|Savills|Hamptons|Chestertons|Strutt & Parker|[\w\s&]+?)\s+(?:drops?|reduces?|lowers?|cuts?|increases?|raises?)(?:\s+by)?\s+(\d+\.?\d*)%'
                    match = re.search(agent_pattern, message_text, re.IGNORECASE)
                    
                    if match:
                        initiating_agent = match.group(1).strip()
                        magnitude = float(match.group(2))
                        
                        # Determine if increase or decrease
                        if any(word in message_text.lower() for word in ['drop', 'reduce', 'lower', 'cut']):
                            magnitude = -magnitude
                        
                        # Run cascade prediction
                        cascade = predict_cascade(network, initiating_agent, magnitude)
                        if not cascade.get('error'):
                            dataset['cascade_prediction'] = cascade
                            logger.info(f"Cascade predicted: {initiating_agent} {magnitude:+.1f}% -> {cascade['total_affected_agents']} agents affected")
        except (ImportError, Exception) as e:
            logger.debug(f"Cascade predictor unavailable: {str(e)}")
        
        # Classify and respond (with client context) - V3 returns metadata
        category, response_text, response_metadata = await classify_and_respond(
            message_text, 
            dataset,
            client_profile=client_profile
        )
        
        # Format response
        formatted_response = format_analyst_response(response_text, category)
        
        # Send via Twilio (with smart chunking)
        await send_twilio_message(sender, formatted_response)
        
        # Log interaction and update client history
        log_interaction(sender, message_text, category, formatted_response)
        update_client_history(sender, message_text, category, preferred_region)
        
        logger.info(f"Message processed: {category} | Confidence: {response_metadata.get('confidence_level')} | Urgency: {response_metadata.get('recommendation_urgency')}")
        
    except Exception as e:
        logger.error(f"Error handling message: {str(e)}", exc_info=True)
        error_msg = "Unable to process your request at this time. Please try again shortly."
        await send_twilio_message(sender, error_msg)


async def send_twilio_message(recipient: str, message: str):
    """Send message via Twilio WhatsApp API with intelligent chunking"""
    try:
        if not twilio_client:
            logger.error("Twilio client not initialized")
            return
        
        MAX_LENGTH = 1500
        
        if len(message) <= MAX_LENGTH:
            # Short message - send as-is
            twilio_client.messages.create(
                from_=TWILIO_WHATSAPP_NUMBER,
                to=recipient,
                body=message
            )
            logger.info(f"Message sent successfully to {recipient} ({len(message)} chars)")
        else:
            # Long message - intelligent splitting
            chunks = smart_split_message(message, MAX_LENGTH)
            
            for i, chunk in enumerate(chunks, 1):
                twilio_client.messages.create(
                    from_=TWILIO_WHATSAPP_NUMBER,
                    to=recipient,
                    body=chunk
                )
                
                # Small delay to maintain order
                import asyncio
                await asyncio.sleep(0.5)
            
            logger.info(f"Multi-part message sent to {recipient} ({len(chunks)} parts, {len(message)} total chars)")
                
    except Exception as e:
        logger.error(f"Error sending Twilio message: {str(e)}", exc_info=True)
        raise


def smart_split_message(message: str, max_length: int) -> list:
    """
    Split message intelligently at natural break points.
    Aims for roughly equal-sized chunks, never orphan headers.
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
        if double_break > max_length * 0.5:  # Only if >50% through chunk
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
                split_point = sentence_end + 1  # Include the period
        
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
    
    # Post-process: If first chunk is tiny (<200 chars), merge with second
    if len(chunks) > 1 and len(chunks[0]) < 200:
        chunks[0] = f"{chunks[0]}\n\n{chunks[1]}"
        chunks.pop(1)
    
    return chunks
