"""
VOXMILL INSTANT RESPONSE LAYER
==============================
Sub-3-second responses using ALL intelligence layers

World-class principle: Surface ALL available intelligence, always.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Optional, List

logger = logging.getLogger(__name__)


class InstantIntelligence:
    """
    Lightning-fast responses that USE ALL INTELLIGENCE LAYERS
    
    No GPT-4 delays - structured templates with full data utilization
    """
    
    @staticmethod
    def get_full_market_snapshot(area: str, dataset: Dict, client_profile: Dict) -> str:
        """
        WORLD-CLASS MARKET SNAPSHOT - Uses ALL intelligence layers
        
        Returns structured response with:
        - Core metrics
        - Detected trends (if available)
        - Agent behavioral signals (if available)
        - Liquidity velocity (if available)
        - Liquidity windows (if available)
        - Micromarket breakdown (if available)
        - Behavioral clusters (if available)
        - Cascade prediction (if available)
        - Proactive suggestions
        """
        
        metrics = dataset.get('metrics', {})
        intelligence = dataset.get('intelligence', {})
        
        # Core metrics
        inventory = metrics.get('property_count', 0)
        avg_price = metrics.get('avg_price', 0)
        price_per_sqft = metrics.get('avg_price_per_sqft', 0)
        sentiment = intelligence.get('market_sentiment', 'neutral').upper()
        
        # Build response sections
        sections = []
        
        # ========================================
        # SECTION 1: CORE SNAPSHOT (Always shown)
        # ========================================
        sections.append(f"""{area.upper()} SNAPSHOT
────────────────────────────────────────
Inventory: {inventory} units
Avg: £{avg_price:,.0f} | £{price_per_sqft:,.0f}/sqft
Sentiment: {sentiment}""")
        
        # ========================================
        # SECTION 2: DETECTED TRENDS (Show if available)
        # ========================================
        if 'detected_trends' in dataset and dataset['detected_trends']:
            trends = dataset['detected_trends'][:3]  # Top 3
            
            trend_bullets = []
            for trend in trends:
                insight = trend.get('insight', '')
                magnitude = trend.get('magnitude', 0)
                
                # Add magnitude indicator
                if magnitude > 0:
                    indicator = f"(↑{abs(magnitude):.1f}%)"
                elif magnitude < 0:
                    indicator = f"(↓{abs(magnitude):.1f}%)"
                else:
                    indicator = ""
                
                trend_bullets.append(f"• {insight} {indicator}")
            
            sections.append(f"""
🔍 DETECTED TRENDS (14d)
{chr(10).join(trend_bullets)}""")
        
        # ========================================
        # SECTION 3: AGENT BEHAVIORAL SIGNALS (Show if available)
        # ========================================
        if 'agent_profiles' in dataset and dataset['agent_profiles']:
            agent_profiles = dataset['agent_profiles'][:5]
            
            # Detect aggressive/defensive agents
            aggressive = [a for a in agent_profiles if 'aggressive' in a.get('archetype', '').lower()]
            defensive = [a for a in agent_profiles if 'defensive' in a.get('archetype', '').lower()]
            
            if aggressive or defensive:
                agent_signals = []
                
                for agent in aggressive[:2]:
                    pattern = agent.get('behavioral_pattern', '')
                    agent_signals.append(f"• {agent['agent']}: {pattern}")
                
                for agent in defensive[:2]:
                    pattern = agent.get('behavioral_pattern', '')
                    agent_signals.append(f"• {agent['agent']}: {pattern}")
                
                sections.append(f"""
⚡ AGENT BEHAVIORAL SIGNALS
{chr(10).join(agent_signals)}""")
        
        # ========================================
        # SECTION 4: LIQUIDITY VELOCITY (Show if available)
        # ========================================
        if 'liquidity_velocity' in dataset and dataset['liquidity_velocity']:
            velocity = dataset['liquidity_velocity']
            
            if not velocity.get('error'):
                velocity_score = velocity.get('velocity_score', 0)
                velocity_class = velocity.get('velocity_class', 'Unknown')
                market_health = velocity.get('market_health', 'Unknown')
                
                sections.append(f"""
💧 LIQUIDITY VELOCITY
Score: {velocity_score}/100 ({velocity_class})
Market health: {market_health}""")
        
        # ========================================
        # SECTION 5: LIQUIDITY WINDOWS (Show if available)
        # ========================================
        if 'liquidity_windows' in dataset and dataset['liquidity_windows']:
            windows = dataset['liquidity_windows']
            
            if not windows.get('error'):
                timing_score = windows.get('timing_score', 0)
                timing_rec = windows.get('timing_recommendation', 'Monitor')
                velocity_momentum = windows.get('velocity_momentum', 0)
                
                # Get next window
                predicted_windows = windows.get('predicted_windows', [])
                if predicted_windows:
                    next_window = predicted_windows[0]
                    window_type = next_window.get('type', 'Unknown')
                    window_status = next_window.get('status', 'Unknown')
                    window_timing = next_window.get('timing', 'Unknown')
                    window_rec = next_window.get('recommendation', '')
                    
                    momentum_indicator = f"({'↑' if velocity_momentum > 0 else '↓'}{abs(velocity_momentum):.1f}%)"
                    
                    sections.append(f"""
⏰ TIMING SIGNAL
Timing score: {timing_score}/100 ({timing_rec})
Momentum: {momentum_indicator}

Next window: {window_type.upper()} - {window_status}
Timing: {window_timing}
Action: {window_rec}""")
        
        # ========================================
        # SECTION 6: MICROMARKET BREAKDOWN (Show if available)
        # ========================================
        if 'micromarkets' in dataset and dataset['micromarkets']:
            micromarkets = dataset['micromarkets']
            
            if not micromarkets.get('error'):
                zones = micromarkets.get('micromarkets', [])[:3]  # Top 3
                
                if zones:
                    zone_bullets = []
                    for zone in zones:
                        zone_name = zone.get('name', 'Unknown')
                        zone_avg = zone.get('avg_price', 0)
                        zone_count = zone.get('property_count', 0)
                        
                        zone_bullets.append(f"• {zone_name}: £{zone_avg:,.0f} ({zone_count} units)")
                    
                    sections.append(f"""
🗺️ MICROMARKET ZONES
{chr(10).join(zone_bullets)}""")
        
        # ========================================
        # SECTION 7: BEHAVIORAL CLUSTERS (Show if available)
        # ========================================
        if 'behavioral_clusters' in dataset and dataset['behavioral_clusters']:
            clusters = dataset['behavioral_clusters']
            
            if not clusters.get('error'):
                cluster_list = clusters.get('clusters', [])[:2]  # Top 2
                
                if cluster_list:
                    cluster_bullets = []
                    for cluster in cluster_list:
                        archetype = cluster.get('archetype', 'Unknown')
                        agents = ', '.join(cluster.get('agents', [])[:3])
                        cohesion = cluster.get('cohesion', 0)
                        
                        cluster_bullets.append(f"• {archetype}: {agents} (cohesion: {cohesion*100:.0f}%)")
                    
                    # Leader-follower relationships
                    leader_follower = clusters.get('leader_follower_pairs', [])[:2]
                    if leader_follower:
                        for pair in leader_follower:
                            leader = pair.get('leader', '')
                            follower = pair.get('follower', '')
                            correlation = pair.get('correlation', 0)
                            
                            cluster_bullets.append(f"• {leader} → {follower} ({correlation*100:.0f}% correlation)")
                    
                    sections.append(f"""
🔗 BEHAVIORAL CLUSTERS
{chr(10).join(cluster_bullets)}""")
        
        # ========================================
        # SECTION 8: CASCADE PREDICTION (Show if available)
        # ========================================
        if 'cascade_prediction' in dataset and dataset['cascade_prediction']:
            cascade = dataset['cascade_prediction']
            
            if not cascade.get('error'):
                initiating_agent = cascade.get('initiating_agent', '')
                initial_magnitude = cascade.get('initial_magnitude', 0)
                cascade_probability = cascade.get('cascade_probability', 0)
                affected_agents = cascade.get('total_affected_agents', 0)
                duration = cascade.get('expected_duration_days', 0)
                market_impact = cascade.get('market_impact', 'unknown').upper()
                
                sections.append(f"""
🌊 CASCADE PREDICTION
Initiator: {initiating_agent} ({initial_magnitude:+.1f}%)
Cascade probability: {cascade_probability*100:.0f}%
Affected agents: {affected_agents}
Timeline: {duration} days
Market impact: {market_impact}""")
        
        # ========================================
        # SECTION 9: PROACTIVE SUGGESTIONS (Always show)
        # ========================================
        suggestions = []
        
        # Suggest monitoring if velocity declining
        if 'liquidity_velocity' in dataset and dataset['liquidity_velocity']:
            velocity_data = dataset['liquidity_velocity']
            if not velocity_data.get('error'):
                velocity_score = velocity_data.get('velocity_score', 50)
                
                if velocity_score < 40:
                    suggestions.append("Monitor liquidity: Currently low, may signal entry opportunity when it recovers")
                elif velocity_score > 70:
                    suggestions.append("Monitor liquidity: Currently high, window may be closing soon")
        
        # Suggest cascade analysis if aggressive agents detected
        if 'agent_profiles' in dataset and dataset['agent_profiles']:
            aggressive = [a for a in dataset['agent_profiles'] if 'aggressive' in a.get('archetype', '').lower()]
            if aggressive and not ('cascade_prediction' in dataset):
                suggestions.append(f"Cascade analysis available: {aggressive[0]['agent']} showing aggressive pattern")
        
        # Suggest portfolio tracking
        suggestions.append("Portfolio tracking available: Track your holdings with real-time valuations")
        
        if suggestions:
            sections.append(f"""
💡 AVAILABLE INTELLIGENCE
{chr(10).join(['• ' + s for s in suggestions[:3]])}""")
        
        # ========================================
        # FINAL ASSEMBLY
        # ========================================
        sections.append("\nStanding by.")
        
        return "\n".join(sections)
    
    @staticmethod
    def get_instant_decision(area: str, dataset: Dict, client_profile: Dict) -> str:
        """
        INSTANT DECISION using liquidity windows + velocity + sentiment
        
        Uses rule-based logic for speed, GPT-4 enhancement optional
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
        response = f"""🎯 DECISION MODE

RECOMMENDATION:
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
            return f"""{area.upper()} TRENDS

No significant trends detected in last 14 days.

Market activity: Stable
Movement: Minimal

Standing by."""
        
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
        
        response = f"""{area.upper()} TREND ANALYSIS (14d)
────────────────────────────────────────

{chr(10).join(trend_bullets)}

Standing by."""
        
        return response
    
    @staticmethod
    def get_timing_analysis(area: str, dataset: Dict) -> str:
        """
        INSTANT TIMING ANALYSIS - Show liquidity windows
        """
        
        if 'liquidity_windows' not in dataset or not dataset['liquidity_windows']:
            return f"""{area.upper()} TIMING ANALYSIS

Liquidity window data unavailable.

Requires historical velocity tracking (30+ days).

Standing by."""
        
        windows = dataset['liquidity_windows']
        
        if windows.get('error'):
            return f"""{area.upper()} TIMING ANALYSIS

{windows['error']}

Standing by."""
        
        # Extract data
        timing_score = windows.get('timing_score', 0)
        timing_rec = windows.get('timing_recommendation', 'Monitor')
        current_velocity = windows.get('current_velocity', 0)
        velocity_momentum = windows.get('velocity_momentum', 0)
        
        # Predicted windows
        predicted_windows = windows.get('predicted_windows', [])
        
        # Build response
        sections = [f"""{area.upper()} TIMING ANALYSIS
────────────────────────────────────────
Current velocity: {current_velocity}/100
Momentum: {'↑' if velocity_momentum > 0 else '↓'}{abs(velocity_momentum):.1f}%
Timing score: {timing_score}/100 ({timing_rec})"""]
        
        if predicted_windows:
            sections.append("\nPREDICTED WINDOWS:")
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
        
        sections.append("\nStanding by.")
        
        return "\n".join(sections)
    
    @staticmethod
    def get_agent_analysis(area: str, dataset: Dict) -> str:
        """
        INSTANT AGENT ANALYSIS - Show agent behaviors
        """
        
        if 'agent_profiles' not in dataset or not dataset['agent_profiles']:
            return f"""{area.upper()} AGENT ANALYSIS

No agent behavioral data available.

Requires 30+ days of tracking.

Standing by."""
        
        agent_profiles = dataset['agent_profiles']
        
        # Build response
        sections = [f"""{area.upper()} AGENT BEHAVIORAL ANALYSIS
────────────────────────────────────────"""]
        
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
⚠️ TACTICAL SIGNAL
{aggressive[0]['agent']} showing aggressive pricing.
Cascade analysis available: "What if {aggressive[0]['agent']} drops 5%?"
""")
        
        sections.append("Standing by.")
        
        return "\n".join(sections)


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
        'decision mode'
    ]
    
    message_lower = message.lower()
    
    # Check if any instant pattern matches
    return any(p in message_lower for p in instant_patterns)
