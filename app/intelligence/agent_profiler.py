"""
Agent Behavior Profiling Engine
Classifies agents into behavioral archetypes based on historical patterns
"""

AGENT_ARCHETYPES = { 
    'momentum_follower': {
        'pattern': 'Matches competitor moves within 7-14 days',
        'trigger': 'Waits for 2+ agents to move first',
        'risk_profile': 'Conservative, market-responsive',
        'prediction_confidence': 0.85
    },
    'market_leader': {
        'pattern': 'Initiates price moves, others follow',
        'trigger': 'Moves independently based on internal signals',
        'risk_profile': 'Aggressive, trend-setting',
        'prediction_confidence': 0.72
    },
    'premium_holder': {
        'pattern': 'Maintains 8-15% premium regardless of market',
        'trigger': 'Only moves under severe pressure (>20% market drop)',
        'risk_profile': 'Ultra-premium positioning, stubborn',
        'prediction_confidence': 0.91
    },
    'opportunist': {
        'pattern': 'Aggressively drops prices during distress',
        'trigger': 'Inventory >60 days, drops 10-15%',
        'risk_profile': 'Volume-focused, flexible',
        'prediction_confidence': 0.68
    },
    'institutional': {
        'pattern': 'Quarterly adjustments, slow-moving',
        'trigger': 'Major macro events (rate changes, policy)',
        'risk_profile': 'Data-driven, predictable',
        'prediction_confidence': 0.88
    }
}

def classify_agent_archetype(agent: str, history: list) -> dict:
    """
    Classify agent into behavioral archetype
    Returns: archetype, confidence, supporting evidence
    """
    
    # Analyze response time to competitor moves
    response_times = []
    for event in history:
        if event['type'] == 'response_to_competitor':
            response_times.append(event['days_to_respond'])
    
    avg_response_time = sum(response_times) / len(response_times) if response_times else 999
    
    # Analyze price positioning
    premium_maintenance = []
    for snapshot in history:
        if 'market_avg' in snapshot and 'agent_avg' in snapshot:
            premium_pct = ((snapshot['agent_avg'] - snapshot['market_avg']) / snapshot['market_avg']) * 100
            premium_maintenance.append(premium_pct)
    
    avg_premium = sum(premium_maintenance) / len(premium_maintenance) if premium_maintenance else 0
    
    # Analyze move initiation vs following
    initiated_moves = len([e for e in history if e.get('first_mover') == True])
    total_moves = len([e for e in history if e.get('type') == 'price_change'])
    initiation_rate = initiated_moves / total_moves if total_moves > 0 else 0
    
    # CLASSIFICATION LOGIC
  # CLASSIFICATION LOGIC
    archetype = 'institutional'  # Default fallback
    
    if avg_premium >= 10 and len(premium_maintenance) > 0 and len([p for p in premium_maintenance if abs(p - avg_premium) < 3]) / len(premium_maintenance) > 0.7:
        archetype = 'premium_holder'
    elif initiation_rate > 0.6:
        archetype = 'market_leader'
    elif avg_response_time < 10 and initiation_rate < 0.3:
        archetype = 'momentum_follower'
    elif any(e.get('drop_magnitude', 0) > 12 for e in history):
        archetype = 'opportunist'
    
    # Validate archetype exists in AGENT_ARCHETYPES
    if archetype not in AGENT_ARCHETYPES:
        logger.warning(f"Unknown archetype '{archetype}', using institutional")
        archetype = 'institutional'


def predict_agent_response(agent_profile: dict, market_scenario: dict) -> dict:
    """
    Given agent's archetype and market scenario, predict their response
    
    market_scenario = {
        'type': 'competitor_price_drop',
        'magnitude': -8.5,
        'agents_involved': ['Knight Frank', 'Savills'],
        'timeframe': 'last_7_days'
    }
    """
    archetype = agent_profile['archetype']
    
    if archetype == 'momentum_follower':
        num_agents_moved = len(market_scenario.get('agents_involved', []))
        if num_agents_moved >= 2:
            return {
                'predicted_action': 'match_within_14_days',
                'probability': 0.85,
                'magnitude': market_scenario['magnitude'] * 0.9,  # Slightly less aggressive
                'timing': '7-14 days',
                'reasoning': f"Momentum follower pattern: waits for 2+ agents ({num_agents_moved} have moved). High confidence match."
            }
        else:
            return {
                'predicted_action': 'monitor',
                'probability': 0.72,
                'timing': 'awaiting 2nd agent move',
                'reasoning': "Momentum follower requires 2+ agents before responding."
            }
    
    elif archetype == 'premium_holder':
        if market_scenario['magnitude'] < -15:
            return {
                'predicted_action': 'partial_adjustment',
                'probability': 0.68,
                'magnitude': market_scenario['magnitude'] * 0.4,  # Minimal adjustment
                'timing': '21-30 days',
                'reasoning': "Premium holders only adjust under severe pressure (>15% drops). Expects partial, delayed response."
            }
        else:
            return {
                'predicted_action': 'hold_position',
                'probability': 0.91,
                'timing': 'no_action_expected',
                'reasoning': f"Premium holder maintains positioning. {abs(market_scenario['magnitude'])}% drop insufficient to trigger response (requires >15%)."
            }
    
    elif archetype == 'market_leader':
        return {
            'predicted_action': 'independent_strategy',
            'probability': 0.65,
            'timing': 'unpredictable',
            'reasoning': "Market leaders move independently. May counter-position or lead next wave. Monitor closely."
        }
    
    elif archetype == 'opportunist':
        if market_scenario.get('market_stress', False):
            return {
                'predicted_action': 'aggressive_drop',
                'probability': 0.78,
                'magnitude': market_scenario['magnitude'] * 1.3,  # More aggressive
                'timing': '3-7 days',
                'reasoning': "Opportunist detects weakness. Expects aggressive undercutting to capture volume."
            }
        else:
            return {
                'predicted_action': 'match_or_exceed',
                'probability': 0.71,
                'magnitude': market_scenario['magnitude'] * 1.1,
                'timing': '5-10 days',
                'reasoning': "Opportunist responds quickly and slightly more aggressively to maintain volume share."
            }
    
    else:  # institutional
        return {
            'predicted_action': 'delayed_data_driven_response',
            'probability': 0.82,
            'magnitude': market_scenario['magnitude'] * 0.7,
            'timing': '30-60 days',
            'reasoning': "Institutional agents adjust quarterly based on data aggregation. Slow but predictable."
        }
