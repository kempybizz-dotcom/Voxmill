import os
import logging
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None


def calculate_liquidity_velocity(properties: list, historical_snapshots: list) -> dict:
    """
    Calculate liquidity velocity - how fast capital is moving through the market
    
    Velocity = (Inventory Turnover) × (Price Dynamism) × (Agent Activity)
    
    Args:
        properties: Current property listings
        historical_snapshots: List of past property snapshots (last 30 days)
    
    Returns: Velocity score with interpretation
    """
    try:
        if not properties or len(properties) < 3:
            return {'error': 'insufficient_current_data'}
        
        if not historical_snapshots or len(historical_snapshots) < 2:
            return {'error': 'insufficient_historical_data', 'message': 'Need at least 2 historical snapshots'}
        
        # 1. INVENTORY TURNOVER RATE
        # How many properties are NEW vs carried over from last snapshot
        current_addresses = set([p.get('address', '') for p in properties if p.get('address')])
        previous_addresses = set([p.get('address', '') for p in historical_snapshots[-1] if p.get('address')])
        
        new_listings = len(current_addresses - previous_addresses)
        carried_over = len(current_addresses & previous_addresses)
        exited_listings = len(previous_addresses - current_addresses)
        
        turnover_rate = (new_listings / len(properties)) * 100 if properties else 0
        
        # 2. PRICE MOVEMENT FREQUENCY
        # How often are prices changing for same properties week-over-week
        price_changes = 0
        price_change_magnitude = []
        
        for prop in properties:
            addr = prop.get('address', '')
            current_price = prop.get('price', 0)
            
            # Find same property in historical data
            historical_match = None
            for hist_prop in historical_snapshots[-1]:
                if hist_prop.get('address', '') == addr:
                    historical_match = hist_prop
                    break
            
            if historical_match and current_price > 0:
                hist_price = historical_match.get('price', 0)
                if hist_price > 0 and abs(current_price - hist_price) > hist_price * 0.01:  # >1% change
                    price_changes += 1
                    change_pct = abs((current_price - hist_price) / hist_price) * 100
                    price_change_magnitude.append(change_pct)
        
        price_dynamism_rate = (price_changes / len(properties)) * 100 if properties else 0
        avg_price_change_magnitude = sum(price_change_magnitude) / len(price_change_magnitude) if price_change_magnitude else 0
        
        # 3. AGENT ACTIVITY LEVEL
        # Number of agents actively listing + diversity of activity
        active_agents = set([p.get('agent', '') for p in properties if p.get('agent') and p.get('agent') != 'Private'])
        num_active_agents = len(active_agents)
        
        # Agent concentration (lower = more diverse = healthier velocity)
        agent_counts = {}
        for prop in properties:
            agent = prop.get('agent', 'Unknown')
            if agent != 'Private':
                agent_counts[agent] = agent_counts.get(agent, 0) + 1
        
        if agent_counts:
            max_agent_share = max(agent_counts.values()) / len([p for p in properties if p.get('agent') != 'Private'])
            agent_diversity_score = (1 - max_agent_share) * 100  # Higher = more diverse
        else:
            agent_diversity_score = 0
        
        # 4. ABSORPTION RATE
        # How quickly are properties leaving the market
        total_previous = len(previous_addresses)
        absorption_rate = (exited_listings / total_previous) * 100 if total_previous > 0 else 0
        
        # CALCULATE VELOCITY SCORE (0-100)
        velocity_score = (
            (turnover_rate * 0.35) +           # 35% weight: new inventory flowing in
            (price_dynamism_rate * 0.25) +     # 25% weight: pricing flexibility
            (min(num_active_agents / 15, 1) * 20) +  # 20% weight: agent participation (capped at 15)
            (agent_diversity_score * 0.10) +   # 10% weight: market diversity
            (absorption_rate * 0.10)           # 10% weight: exit velocity
        )
        
        # CLASSIFY VELOCITY
        if velocity_score >= 65:
            velocity_class = 'high_velocity'
            interpretation = "Capital rotating rapidly. High liquidity, fast absorption, dynamic pricing."
            market_health = 'strong'
            investor_implication = "Favorable entry/exit conditions. Low transaction friction."
        elif velocity_score >= 40:
            velocity_class = 'moderate_velocity'
            interpretation = "Balanced capital flow. Steady absorption, moderate pricing adjustments."
            market_health = 'stable'
            investor_implication = "Normal transaction environment. Standard due diligence timelines."
        elif velocity_score >= 20:
            velocity_class = 'low_velocity'
            interpretation = "Capital stagnation emerging. Slow turnover, limited price discovery."
            market_health = 'cooling'
            investor_implication = "Extended holding periods likely. Negotiate aggressively on price."
        else:
            velocity_class = 'frozen'
            interpretation = "Market seizing. Minimal inventory movement, price rigidity."
            market_health = 'stressed'
            investor_implication = "Distress opportunities possible. Extreme buyer leverage."
        
        # HISTORICAL COMPARISON (7-day and 30-day averages)
        historical_velocities = []
        for i in range(1, min(len(historical_snapshots), 30)):
            if i < len(historical_snapshots) - 1:
                # Calculate velocity between snapshot i and i-1
                hist_current = historical_snapshots[i]
                hist_previous = historical_snapshots[i-1]
                
                hist_current_addrs = set([p.get('address', '') for p in hist_current if p.get('address')])
                hist_previous_addrs = set([p.get('address', '') for p in hist_previous if p.get('address')])
                
                if hist_previous_addrs:
                    hist_turnover = (len(hist_current_addrs - hist_previous_addrs) / len(hist_current)) * 100 if hist_current else 0
                    historical_velocities.append(hist_turnover * 0.35)  # Simplified proxy
        
        avg_7day_velocity = sum(historical_velocities[:7]) / 7 if len(historical_velocities) >= 7 else velocity_score
        avg_30day_velocity = sum(historical_velocities) / len(historical_velocities) if historical_velocities else velocity_score
        
        # ACCELERATION/DECELERATION
        if velocity_score > avg_7day_velocity * 1.1:
            momentum = 'accelerating'
            momentum_pct = ((velocity_score - avg_7day_velocity) / avg_7day_velocity) * 100
        elif velocity_score < avg_7day_velocity * 0.9:
            momentum = 'decelerating'
            momentum_pct = ((avg_7day_velocity - velocity_score) / avg_7day_velocity) * 100
        else:
            momentum = 'stable'
            momentum_pct = 0
        
        return {
            'velocity_score': round(velocity_score, 1),
            'velocity_class': velocity_class,
            'market_health': market_health,
            'interpretation': interpretation,
            'investor_implication': investor_implication,
            'components': {
                'turnover_rate': round(turnover_rate, 1),
                'new_listings': new_listings,
                'carried_over': carried_over,
                'exited_listings': exited_listings,
                'price_dynamism_rate': round(price_dynamism_rate, 1),
                'avg_price_change_magnitude': round(avg_price_change_magnitude, 1),
                'active_agents': num_active_agents,
                'agent_diversity_score': round(agent_diversity_score, 1),
                'absorption_rate': round(absorption_rate, 1)
            },
            'historical_comparison': {
                '7_day_avg': round(avg_7day_velocity, 1),
                '30_day_avg': round(avg_30day_velocity, 1),
                'momentum': momentum,
                'momentum_pct': round(abs(momentum_pct), 1) if momentum_pct else 0
            },
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
        
    except Exception as e:
        logger.error(f"Error calculating liquidity velocity: {str(e)}", exc_info=True)
        return {'error': 'calculation_failed', 'message': str(e)}


def get_velocity_alerts(current_velocity: dict, threshold: float = 15.0) -> list:
    """
    Generate alerts for significant velocity changes
    
    Args:
        current_velocity: Velocity dict from calculate_liquidity_velocity
        threshold: Percentage change threshold for alerts (default 15%)
    
    Returns: List of alert dicts
    """
    alerts = []
    
    if current_velocity.get('error'):
        return alerts
    
    score = current_velocity.get('velocity_score', 0)
    momentum = current_velocity.get('historical_comparison', {}).get('momentum', 'stable')
    momentum_pct = current_velocity.get('historical_comparison', {}).get('momentum_pct', 0)
    
    # Alert 1: Rapid deceleration (market cooling fast)
    if momentum == 'decelerating' and momentum_pct >= threshold:
        alerts.append({
            'type': 'velocity_deceleration',
            'severity': 'high' if momentum_pct >= 25 else 'medium',
            'message': f"Liquidity velocity declining {momentum_pct:.1f}% vs 7-day avg—market cooling rapidly",
            'implication': "Extended transaction timelines likely. Consider aggressive pricing for exits.",
            'confidence': 0.82
        })
    
    # Alert 2: Rapid acceleration (market heating)
    if momentum == 'accelerating' and momentum_pct >= threshold:
        alerts.append({
            'type': 'velocity_acceleration',
            'severity': 'medium',
            'message': f"Liquidity velocity increasing {momentum_pct:.1f}% vs 7-day avg—market heating",
            'implication': "Competition for assets intensifying. Act decisively on opportunities.",
            'confidence': 0.78
        })
    
    # Alert 3: Frozen market (extreme low velocity)
    if current_velocity.get('velocity_class') == 'frozen':
        alerts.append({
            'type': 'market_freeze',
            'severity': 'critical',
            'message': f"Market velocity critically low ({score:.1f}/100)—capital flow stagnant",
            'implication': "Distress opportunities possible. Extreme buyer leverage. Long holding periods.",
            'confidence': 0.91
        })
    
    # Alert 4: Absorption collapse
    absorption = current_velocity.get('components', {}).get('absorption_rate', 0)
    if absorption < 5:
        alerts.append({
            'type': 'absorption_collapse',
            'severity': 'high',
            'message': f"Absorption rate critically low ({absorption:.1f}%)—inventory stagnation",
            'implication': "Properties not exiting market. Price discovery impaired.",
            'confidence': 0.85
        })
    
    # Alert 5: High velocity with low price dynamism (potential bubble)
    turnover = current_velocity.get('components', {}).get('turnover_rate', 0)
    price_dynamism = current_velocity.get('components', {}).get('price_dynamism_rate', 0)
    
    if turnover > 40 and price_dynamism < 15:
        alerts.append({
            'type': 'potential_bubble',
            'severity': 'medium',
            'message': "High turnover with rigid pricing—potential froth",
            'implication': "Market momentum divorced from price discovery. Monitor for correction signals.",
            'confidence': 0.65
        })
    
    return alerts


def compare_velocity_across_markets(market_velocities: dict) -> list:
    """
    Compare velocity across multiple markets to identify divergences
    
    Args:
        market_velocities: {
            'Mayfair': velocity_dict,
            'Knightsbridge': velocity_dict,
            ...
        }
    
    Returns: List of comparative insights
    """
    insights = []
    
    if len(market_velocities) < 2:
        return insights
    
    # Sort markets by velocity score
    sorted_markets = sorted(
        market_velocities.items(),
        key=lambda x: x[1].get('velocity_score', 0) if not x[1].get('error') else 0,
        reverse=True
    )
    
    # Identify leaders and laggards
    if len(sorted_markets) >= 2:
        leader = sorted_markets[0]
        laggard = sorted_markets[-1]
        
        leader_score = leader[1].get('velocity_score', 0)
        laggard_score = laggard[1].get('velocity_score', 0)
        
        if leader_score > 0:
            gap_pct = ((leader_score - laggard_score) / laggard_score) * 100
            
            if gap_pct > 30:
                insights.append({
                    'type': 'market_divergence',
                    'leader': leader[0],
                    'laggard': laggard[0],
                    'gap_pct': round(gap_pct, 1),
                    'insight': f"{leader[0]} velocity {gap_pct:.0f}% higher than {laggard[0]}—significant capital flow divergence",
                    'implication': f"Capital rotating into {leader[0]}, away from {laggard[0]}. Monitor for reversal signals."
                })
    
    # Identify accelerating markets
    accelerating = [
        (name, data) for name, data in market_velocities.items()
        if not data.get('error') and data.get('historical_comparison', {}).get('momentum') == 'accelerating'
    ]
    
    if len(accelerating) >= 2:
        insights.append({
            'type': 'multi_market_acceleration',
            'markets': [m[0] for m in accelerating],
            'insight': f"{len(accelerating)} markets accelerating simultaneously—broad capital deployment",
            'implication': "Institutional or macro-driven momentum. Consider sector-wide positioning."
        })
    
    return insights
