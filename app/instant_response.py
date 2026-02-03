"""
VOXMILL INSTANT RESPONSE LAYER - WORLD-CLASS v2.0
==================================================
Sub-3-second responses with executive-grade formatting

FORMATTING PHILOSOPHY:
- Assume intelligent client (no explanations)
- End on insight, not status
- Dense > decorated
- Authority > helpfulness
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)


class InstantIntelligence:
    """
    Lightning-fast responses with institutional formatting
    
    No GPT-4 delays - structured templates with executive brevity
    """
    
    @staticmethod
    def get_full_market_snapshot(area: str, dataset: Dict, client_profile: Dict) -> str:
        """
        WORLD-CLASS MARKET SNAPSHOT
        
        Format: State → Interpretation → Key Focus
        No filler, no system voice, ends strong
        """
        
        metrics = dataset.get('metrics', {})
        intelligence = dataset.get('intelligence', {})
        
        # Core metrics
        inventory = metrics.get('property_count', 0)
        avg_price = metrics.get('avg_price', 0)
        price_per_sqft = metrics.get('avg_price_per_sqft', 0)
        sentiment = intelligence.get('market_sentiment', 'neutral').upper()
        
        # Convert price to millions for readability
        avg_price_m = avg_price / 1_000_000
        
        # Build response sections
        sections = []
        
        # ========================================
        # SECTION 1: CORE STATE (Always shown)
        # ========================================
        sections.append(f"""- Inventory: {inventory} units  
- Avg pricing: £{avg_price_m:.2f}m  
- Sentiment: {sentiment}""")
        
        # ========================================
        # SECTION 2: LIQUIDITY VELOCITY (If available)
        # ========================================
        if 'liquidity_velocity' in dataset and dataset['liquidity_velocity']:
            velocity = dataset['liquidity_velocity']
            
            if not velocity.get('error'):
                velocity_score = velocity.get('velocity_score', 0)
                velocity_class = velocity.get('velocity_class', 'Unknown')
                market_health = velocity.get('market_health', 'Unknown')
                
                sections.append(f"""• Velocity: {velocity_score}/100 ({velocity_class})""")
        
        # ========================================
        # SECTION 3: MICROMARKET BREAKDOWN (If available)
        # ========================================
        if 'micromarkets' in dataset and dataset['micromarkets']:
            micromarkets = dataset['micromarkets']
            
            if not micromarkets.get('error'):
                zones = micromarkets.get('micromarkets', [])[:3]  # Top 3
                
                if zones:
                    sections.append("\nTop zones:")
                    for zone in zones:
                        zone_name = zone.get('name', 'Unknown')
                        zone_avg = zone.get('avg_price', 0) / 1_000_000
                        zone_count = zone.get('property_count', 0)
                        
                        sections.append(f"• {zone_name}: £{zone_avg:.2f}m ({zone_count} units)")
        
        # ========================================
        # SECTION 4: INTERPRETATION (Always show)
        # ========================================
        
        # Derive interpretation from velocity + sentiment
        interpretation = ""
        
        if 'liquidity_velocity' in dataset and dataset['liquidity_velocity']:
            velocity_data = dataset['liquidity_velocity']
            if not velocity_data.get('error'):
                velocity_score = velocity_data.get('velocity_score', 50)
                
                if velocity_score < 40:
                    if sentiment.lower() == 'bearish':
                        interpretation = "Demand is weak, transactions are slow. Risk is entry timing—illiquid declining assets lock capital."
                    else:
                        interpretation = "Demand is stable, but transactions are slow. Risk is timing, not pricing."
                elif velocity_score > 70:
                    interpretation = "Transactions are accelerating. Window is open but narrowing—momentum will reverse."
                else:
                    interpretation = "Standard market conditions. No urgency, no major red flags."
        else:
            # Fallback interpretation from sentiment only
            if sentiment.lower() == 'bearish':
                interpretation = "Sentiment is bearish. Monitor for trend reversal before entry."
            elif sentiment.lower() == 'bullish':
                interpretation = "Sentiment is bullish. Watch for overheating signals."
            else:
                interpretation = "Market is balanced. Standard entry conditions."
        
        if interpretation:
            sections.append(f"\nInterpretation:\n{interpretation}")
        
        # ========================================
        # SECTION 5: KEY FOCUS (Always show - ends strong)
        # ========================================
        
        key_focus = ""
        
        # Determine focus based on available intelligence
        if 'liquidity_velocity' in dataset and dataset['liquidity_velocity']:
            velocity_data = dataset['liquidity_velocity']
            if not velocity_data.get('error'):
                velocity_score = velocity_data.get('velocity_score', 50)
                
                if velocity_score < 40:
                    key_focus = "Watch velocity inflection—that's where leverage returns."
                elif velocity_score > 70:
                    key_focus = "Monitor velocity peak—window closes when momentum reverses."
                else:
                    key_focus = "Track velocity shifts—positioning matters more than timing."
        
        # Add cascade warning if aggressive agents detected
        if 'agent_profiles' in dataset and dataset['agent_profiles']:
            aggressive = [a for a in dataset['agent_profiles'] if 'aggressive' in a.get('archetype', '').lower()]
            if aggressive and not key_focus:
                agent_name = aggressive[0]['agent']
                key_focus = f"Monitor {agent_name}—aggressive pricing may trigger cascade."
        
        # Fallback if no specific focus identified
        if not key_focus:
            key_focus = "Monitor for structural shifts—timing beats precision here."
        
        sections.append(f"\nKey focus:\n{key_focus}")
        
        # ========================================
        # FINAL ASSEMBLY (NO "Standing by.")
        # ========================================
        
        return "\n".join(sections)
    
    @staticmethod
    def get_instant_decision(area: str, dataset: Dict, client_profile: Dict) -> str:
        """
        INSTANT DECISION using liquidity windows + velocity + sentiment
        
        Format: Recommendation → Risk → Counterfactual → Action
        """
        
        intelligence = dataset.get('intelligence', {})
        sentiment = intelligence.get('market_sentiment', 'neutral')
        
        # Get liquidity data
        velocity_data = dataset.get('liquidity_velocity', {})
        velocity_score = velocity_data.get('velocity_score', 50)
        
        windows_data = dataset.get('liquidity_windows', {})
        timing_recommendation = windows_data.get('timing_recommendation', 'Monitor')
        velocity_momentum = windows_data.get('velocity_momentum', 0)
        
        # RULE-BASED DECISION MATRIX
        if velocity_score > 70 and velocity_momentum > 0:
            # High velocity, improving
            recommendation = "Acquire immediately. Liquidity window open but closing fast."
            risk = "Window closes in 7-14 days, entry cost increases"
            counterfactual = [
                "Day 7: Velocity peaks then reverses, entry cost +£50k-£100k per unit",
                "Day 14: Window closes, optimal pricing lost, cost +£150k-£250k",
                "Day 30: Market normalizes, total opportunity cost -£300k-£500k"
            ]
            action = "Execute within 48 hours"
        
        elif velocity_score > 70 and velocity_momentum < 0:
            # High velocity, declining
            recommendation = "URGENT ENTRY. Liquidity peak reached, declining now."
            risk = "Optimal window is RIGHT NOW, closes within 7 days"
            counterfactual = [
                "Day 3: Velocity drops below 65, negotiation leverage weakens",
                "Day 7: Window closed, entry cost +£100k-£180k per unit",
                "Day 14: Forced to wait for next window (21-30 days out)"
            ]
            action = "Execute TODAY if positioned"
        
        elif velocity_score < 40 and sentiment == 'bearish':
            # Low velocity, bearish = BAD entry
            recommendation = "HOLD CAPITAL. Market illiquid, unfavorable entry conditions."
            risk = "Entering now = locked into illiquid declining asset"
            counterfactual = [
                "Day 7: If you enter now, 10-15 day velocity penalty on exit",
                "Day 14: Liquidity may recover to 50-60 (better entry then)",
                "Day 30: Patient capital wins, premature entry costs -£80k-£150k"
            ]
            action = "Wait for velocity >60 or sentiment shift to neutral"
        
        elif 50 <= velocity_score <= 70:
            # Moderate velocity
            recommendation = "Standard entry acceptable. No urgency, but no major red flags."
            risk = "Moderate opportunity cost if market accelerates"
            counterfactual = [
                "Day 7: Minimal cost impact if delayed (-£10k-£25k)",
                "Day 14: Standard market movement, no major penalty",
                "Day 30: If velocity improves, slight missed opportunity (-£40k-£70k)"
            ]
            action = "Proceed with standard diligence timeline"
        
        else:
            # Default case
            recommendation = "Monitor and wait. Market conditions unclear."
            risk = "Uncertain timing"
            counterfactual = [
                "Day 7: Monitor velocity and sentiment shifts",
                "Day 14: Re-assess liquidity windows",
                "Day 30: Better data will clarify optimal entry"
            ]
            action = "Continue monitoring, set velocity alert at 60+"
        
        # Build response
        response = f"""RECOMMENDATION:
{recommendation}

PRIMARY RISK:
{risk}

COUNTERFACTUAL (If you don't act):
{chr(10).join(['• ' + cf for cf in counterfactual])}

ACTION:
{action}"""
        
        return response
    
    @staticmethod
    def get_trend_analysis(area: str, dataset: Dict) -> str:
        """
        INSTANT TREND ANALYSIS - Show detected trends
        """
        
        if 'detected_trends' not in dataset or not dataset['detected_trends']:
            return """No significant trends detected in last 14 days.
Market activity: Stable, minimal movement."""
        
        trends = dataset['detected_trends']
        
        # Build response
        trend_bullets = []
        for trend in trends[:5]:
            insight = trend.get('insight', '')
            magnitude = trend.get('magnitude', 0)
            confidence = trend.get('confidence', 0)
            
            # Add indicators
            if magnitude > 0:
                indicator = f"↑{abs(magnitude):.1f}%"
            elif magnitude < 0:
                indicator = f"↓{abs(magnitude):.1f}%"
            else:
                indicator = "→"
            
            trend_bullets.append(f"• {insight} ({indicator}, {confidence*100:.0f}% confidence)")
        
        response = f"""{chr(10).join(trend_bullets)}"""
        
        return response
    
    @staticmethod
    def get_timing_analysis(area: str, dataset: Dict) -> str:
        """
        INSTANT TIMING ANALYSIS - Show liquidity windows
        """
        
        if 'liquidity_windows' not in dataset or not dataset['liquidity_windows']:
            return """Liquidity window data unavailable.
Requires 30+ days historical tracking."""
        
        windows = dataset['liquidity_windows']
        
        if windows.get('error'):
            return f"""{windows['error']}"""
        
        # Extract data
        timing_score = windows.get('timing_score', 0)
        timing_rec = windows.get('timing_recommendation', 'Monitor')
        current_velocity = windows.get('current_velocity', 0)
        velocity_momentum = windows.get('velocity_momentum', 0)
        
        # Predicted windows
        predicted_windows = windows.get('predicted_windows', [])
        
        # Build response
        sections = [f"""Current velocity: {current_velocity}/100
Momentum: {'↑' if velocity_momentum > 0 else '↓'}{abs(velocity_momentum):.1f}%
Timing score: {timing_score}/100 ({timing_rec})"""]
        
        if predicted_windows:
            sections.append("\nPredicted windows:")
            for window in predicted_windows[:3]:
                window_type = window.get('type', 'Unknown')
                status = window.get('status', 'Unknown')
                timing = window.get('timing', 'Unknown')
                recommendation = window.get('recommendation', '')
                confidence = window.get('confidence', 0)
                
                sections.append(f"""
- {window_type.upper()}: {status}
  Timing: {timing}
  Action: {recommendation}
  Confidence: {confidence*100:.0f}%""")
        
        return "\n".join(sections)
    
    @staticmethod
    def get_agent_analysis(area: str, dataset: Dict) -> str:
        """
        INSTANT AGENT ANALYSIS - Show agent behaviors
        """
        
        if 'agent_profiles' not in dataset or not dataset['agent_profiles']:
            return """No agent behavioral data available.
Requires 30+ days of tracking."""
        
        agent_profiles = dataset['agent_profiles']
        
        # Build response
        sections = []
        
        for agent in agent_profiles[:5]:
            agent_name = agent.get('agent', 'Unknown')
            archetype = agent.get('archetype', 'Unknown')
            pattern = agent.get('behavioral_pattern', '')
            confidence = agent.get('confidence', 0)
            
            sections.append(f"""
- {agent_name}: {archetype.upper()}
  Pattern: {pattern}
  Confidence: {confidence*100:.0f}%""")
        
        # Add cascade warning if aggressive agents detected
        aggressive = [a for a in agent_profiles if 'aggressive' in a.get('archetype', '').lower()]
        if aggressive:
            sections.append(f"""
TACTICAL SIGNAL
{aggressive[0]['agent']} showing aggressive pricing.
Cascade analysis available: "What if {aggressive[0]['agent']} drops 5%?"
""")
        
        return "\n".join(sections)
    
    @staticmethod
    def get_net_position(area: str, dataset: Dict) -> str:
        """
        NET POSITION - Structured decision format
        
        Format: Position → Why → Upside Trigger → Downside Risk
        Maximum clarity, zero decoration
        """
        
        metrics = dataset.get('metrics', {})
        intelligence = dataset.get('intelligence', {})
        
        # Get key metrics
        inventory = metrics.get('property_count', 0)
        avg_price = metrics.get('avg_price', 0)
        sentiment = intelligence.get('market_sentiment', 'neutral').lower()
        
        # Get velocity if available
        velocity_data = dataset.get('liquidity_velocity', {})
        velocity_score = velocity_data.get('velocity_score', 50)
        velocity_class = velocity_data.get('velocity_class', 'moderate').lower()
        
        # DETERMINE NET POSITION (rule-based logic)
        if velocity_score > 70 and sentiment in ['bullish', 'neutral']:
            position = "Long"
            why = f"Velocity {velocity_score}/100 with {inventory} units—window open"
            upside = "Coordinated price increases if velocity sustains >75"
            downside = "Velocity reversal below 60 closes entry window"
        
        elif velocity_score < 35:
            position = "Hold"
            why = f"Velocity sub-35 with flat inventory—illiquid conditions"
            upside = "Velocity recovery >50 reopens entry"
            downside = f"Extended DOM >60 days in £{int(avg_price/1_000_000):.0f}m band locks capital"
        
        elif sentiment == 'bearish' and velocity_score < 50:
            position = "Short (Exit bias)"
            why = f"Bearish sentiment with velocity {velocity_score}/100—deteriorating"
            upside = "Sentiment shift to neutral + velocity >55"
            downside = "Continued decline, liquidity dries up further"
        
        else:
            # Neutral default
            position = "Neutral"
            why = f"Velocity {velocity_score}/100, sentiment {sentiment}—balanced"
            upside = f"Velocity >60 or first coordinated price cuts"
            downside = f"Velocity sustained <40 or inventory spike >15%"
        
        # Build response
        return f"""NET POSITION: {position}

Why: {why}

Upside trigger: {upside}

Downside risk: {downside}"""

    @staticmethod
    def get_blind_spot_analysis(area: str, dataset: Dict, client_profile: Dict = None) -> str:
        """
        BLIND SPOT ANALYSIS for meta-strategic queries
    
        ✅ CHATGPT FIX: NEVER returns acknowledgement-only responses
        Format: Gap → Why → Resolution signal
        """
    
        metrics = dataset.get('metrics', {})
        intelligence = dataset.get('intelligence', {})
    
        # Get velocity for context
        velocity_data = dataset.get('liquidity_velocity', {})
        velocity_score = velocity_data.get('velocity_score', 50)
        sentiment = intelligence.get('market_sentiment', 'neutral')
    
        # STRUCTURED BLIND SPOT ANALYSIS (3 parts always)
    
        # Part 1: The Gap (what's not visible)
        gap = "Off-market activity not visible in live listings yet"
    
        # Part 2: Why it matters
        why = "Sentiment shifts before transaction data moves—by the time pricing reflects it, positioning window has closed"
    
        # Part 3: Resolution signal
        if velocity_score < 40:
            resolution = f"Monitor velocity (currently {velocity_score}/100)—first acceleration above 50 signals hidden demand surfacing"
        elif velocity_score > 70:
            resolution = f"Monitor for velocity peak—currently {velocity_score}/100, watch for decline as hidden supply enters"
        else:
            resolution = "Watch for velocity inflection points—that's where hidden activity becomes visible"
    
        return f"""You're not missing data — you may be underweighting timing lags.

    Two blind spots to pressure test:
    - {gap}
    - Competitor risk tolerance shifting before pricing moves

    {resolution}"""
        
        @staticmethod
        def get_plain_english_definition(term: str, dataset: Dict) -> str:
            """
            PLAIN ENGLISH DEFINITIONS for "explain like I'm explaining to a client"
            ✅ CHATGPT FIX: No scores, no metrics, one sentence each
            """
            
            definitions = {
                'velocity': "Velocity just means how quickly homes are actually selling. When it's low, buyers hesitate longer and pricing power weakens.",
                'liquidity': "Liquidity is how easy it is to sell without dropping price. High liquidity = properties move fast at asking price.",
                'sentiment': "Sentiment is whether the market feels confident or nervous. Bearish = buyers waiting, sellers anxious.",
            }
            
            term_lower = term.lower()
            for key, definition in definitions.items():
                if key in term_lower:
                    return definition
                    
            return f"No plain English definition available for '{term}'."


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
        'snapshot',
        'trends',
        'timing',
        'agents',
        'decision mode',
        'net position',
        'market position',
        'what am i missing',
        'blind spot',
        'what\'s missing'
        
    ]
    
    message_lower = message.lower()
    
    # Check if any instant pattern matches
    return any(p in message_lower for p in instant_patterns)
