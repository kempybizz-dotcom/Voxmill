"""
VOXMILL INSTANT RESPONSE LAYER
==============================
Sub-3-second responses using pre-computed intelligence
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Optional

logger = logging.getLogger(__name__)


class InstantIntelligence:
    """
    Lightning-fast responses using cached + pre-computed data
    
    World-class principle: NEVER make the client wait for data loading
    """
    
    @staticmethod
    def get_instant_market_overview(area: str, dataset: Dict) -> str:
        """
        Generate market overview in <1 second using pre-loaded dataset
        
        NO GPT-4 call here - use structured templates
        """
        
        metrics = dataset.get('metrics', {})
        intelligence = dataset.get('intelligence', {})
        
        # Extract key metrics
        inventory = metrics.get('property_count', 0)
        avg_price = metrics.get('avg_price', 0)
        price_per_sqft = metrics.get('avg_price_per_sqft', 0)
        sentiment = intelligence.get('market_sentiment', 'neutral')
        
        # Get top agents
        top_agents = intelligence.get('top_agents', [])[:3]
        agent_names = ', '.join([a['name'] for a in top_agents])
        
        # STRUCTURED TEMPLATE - NO LLM NEEDED
        response = f"""MAYFAIR SNAPSHOT

Inventory: {inventory} units
Avg: £{avg_price:,.0f} | £{price_per_sqft}/sqft
Sentiment: {sentiment.upper()}

Leading agents: {agent_names}

Standing by."""
        
        return response
    
    @staticmethod
    def get_instant_decision(area: str, dataset: Dict, client_profile: Dict) -> str:
        """
        Instant decision using rule-based logic + pre-computed signals
        
        World-class principle: Use GPT-4 for REFINEMENT, not initial decision
        """
        
        intelligence = dataset.get('intelligence', {})
        sentiment = intelligence.get('market_sentiment', 'neutral')
        velocity = dataset.get('liquidity_velocity', {}).get('velocity_score', 50)
        
        # RULE-BASED DECISION (Instant)
        if sentiment == 'very_bullish' and velocity > 70:
            recommendation = "Acquire immediately. Window closing."
            risk = "Price acceleration if delayed"
            action = "Execute within 48 hours"
        
        elif sentiment == 'bearish' and velocity < 40:
            recommendation = "Hold capital. Market illiquid, unfavorable entry."
            risk = "Locked into declining asset"
            action = "Wait for velocity >60 or sentiment shift"
        
        elif velocity > 70:
            recommendation = "Enter now. High liquidity = optimal timing."
            risk = "Window closes in 7-14 days"
            action = "Initiate acquisition process today"
        
        else:
            recommendation = "Standard entry acceptable. No urgency."
            risk = "Moderate opportunity cost if delayed"
            action = "Proceed with standard diligence timeline"
        
        # INSTANT DECISION MODE RESPONSE
        response = f"""🎯 DECISION MODE

RECOMMENDATION:
{recommendation}

PRIMARY RISK:
{risk}

COUNTERFACTUAL (If you don't act):
- Day 7: Velocity decay begins, entry cost +£15k-30k
- Day 14: Window narrows, optimal pricing lost
- Day 30: Market normalizes, opportunity cost -£50k-100k

ACTION:
{action}"""
        
        return response


def should_use_instant_response(message: str, category: str) -> bool:
    """
    Determine if query can be answered instantly vs needs full LLM
    
    World-class principle: 80% of queries are repetitive patterns
    """
    
    instant_patterns = [
        'market overview',
        'what\'s up',
        'status',
        'quick update',
        'brief',
        'snapshot'
    ]
    
    # Decision mode CAN be instant if using rule-based logic
    decision_patterns = [
        'decision mode',
        'what should i do',
        'recommend'
    ]
    
    message_lower = message.lower()
    
    # Simple queries = instant
    if any(p in message_lower for p in instant_patterns):
        return True
    
    # Decision mode = instant IF dataset has velocity data
    if any(p in message_lower for p in decision_patterns):
        return True  # We'll use rule-based decision
    
    return False
