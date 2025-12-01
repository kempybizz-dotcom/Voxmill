import os
import logging
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
import json

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv("MONGODB_URI")
mongo_client = MongoClient(MONGODB_URI) if MONGODB_URI else None


def build_agent_network(area: str = "Mayfair", lookback_days: int = 90) -> dict:
    """
    Build directed graph of agent influence relationships
    
    Edge weight = probability that Agent B responds when Agent A moves
    Edge attributes = avg response time, typical magnitude
    
    Args:
        area: Market area
        lookback_days: Historical period to analyze
    
    Returns: Network dict with nodes (agents) and edges (influence relationships)
    """
    try:
        if not mongo_client:
            logger.warning("MongoDB not connected, cannot build agent network")
            return {'error': 'no_database'}
        
        db = mongo_client['Voxmill']
        price_history = db['price_history']
        
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=lookback_days)
        
        # Get all price change events
        events = list(price_history.find({
            'area': area,
            'timestamp': {'$gte': cutoff_date}
        }).sort('timestamp', 1))
        
        if len(events) < 5:
            logger.info(f"Insufficient events for cascade network in {area}")
            return {'error': 'insufficient_data', 'events_found': len(events)}
        
        # Detect price change events (>3% moves)
        price_changes = []
        for i, event in enumerate(events):
            if i == 0:
                continue
            
            agent = event['agent']
            
            # Find previous snapshot for this agent
            prev_event = None
            for j in range(i-1, -1, -1):
                if events[j]['agent'] == agent:
                    prev_event = events[j]
                    break
            
            if prev_event:
                price_change_pct = ((event['avg_price'] - prev_event['avg_price']) / prev_event['avg_price']) * 100
                
                if abs(price_change_pct) >= 3:  # Significant move threshold
                    price_changes.append({
                        'agent': agent,
                        'timestamp': event['timestamp'],
                        'magnitude': price_change_pct,
                        'new_price': event['avg_price'],
                        'old_price': prev_event['avg_price'],
                        'event_index': i
                    })
        
        if len(price_changes) < 3:
            return {'error': 'insufficient_price_changes', 'changes_found': len(price_changes)}
        
        # Build influence graph
        network = {
            'nodes': {},  # agent -> {total_moves, initiations, responses}
            'edges': {},  # (agent_a, agent_b) -> {response_count, avg_days, magnitudes}
            'area': area,
            'analysis_period_days': lookback_days,
            'total_price_events': len(price_changes)
        }
        
        # Initialize nodes
        for change in price_changes:
            agent = change['agent']
            if agent not in network['nodes']:
                network['nodes'][agent] = {
                    'total_moves': 0,
                    'initiations': 0,
                    'responses': 0
                }
            network['nodes'][agent]['total_moves'] += 1
        
        # Analyze response patterns
        for i, change_a in enumerate(price_changes):
            agent_a = change_a['agent']
            timestamp_a = change_a['timestamp']
            
            # Look for responses within 30 days
            for change_b in price_changes[i+1:]:
                agent_b = change_b['agent']
                timestamp_b = change_b['timestamp']
                
                if agent_a == agent_b:
                    continue  # Skip same agent
                
                days_diff = (timestamp_b - timestamp_a).total_seconds() / 86400
                
                if days_diff <= 30:
                    # Agent B moved within 30 days of Agent A
                    edge_key = f"{agent_a}->{agent_b}"
                    
                    if edge_key not in network['edges']:
                        network['edges'][edge_key] = {
                            'from_agent': agent_a,
                            'to_agent': agent_b,
                            'response_count': 0,
                            'response_days': [],
                            'magnitude_ratios': []
                        }
                    
                    network['edges'][edge_key]['response_count'] += 1
                    network['edges'][edge_key]['response_days'].append(days_diff)
                    
                    # Calculate magnitude ratio (how aggressive was B's response)
                    if change_a['magnitude'] != 0:
                        magnitude_ratio = change_b['magnitude'] / change_a['magnitude']
                        network['edges'][edge_key]['magnitude_ratios'].append(magnitude_ratio)
                    
                    # Mark as response (not initiation)
                    network['nodes'][agent_b]['responses'] += 1
        
        # Calculate edge probabilities and averages
        for edge_key, edge_data in network['edges'].items():
            agent_a = edge_data['from_agent']
            total_moves_a = network['nodes'][agent_a]['total_moves']
            
            # Probability = (times B responded to A) / (times A moved)
            edge_data['response_probability'] = edge_data['response_count'] / total_moves_a if total_moves_a > 0 else 0
            edge_data['avg_response_days'] = sum(edge_data['response_days']) / len(edge_data['response_days']) if edge_data['response_days'] else 0
            edge_data['avg_magnitude_ratio'] = sum(edge_data['magnitude_ratios']) / len(edge_data['magnitude_ratios']) if edge_data['magnitude_ratios'] else 1.0
            
            # Clean up lists (keep summary stats only)
            edge_data['response_days_range'] = {
                'min': min(edge_data['response_days']) if edge_data['response_days'] else 0,
                'max': max(edge_data['response_days']) if edge_data['response_days'] else 0
            }
            del edge_data['response_days']
            del edge_data['magnitude_ratios']
        
        # Calculate initiations (moves not in response to others)
        for agent, node_data in network['nodes'].items():
            node_data['initiations'] = node_data['total_moves'] - node_data['responses']
            node_data['initiation_rate'] = node_data['initiations'] / node_data['total_moves'] if node_data['total_moves'] > 0 else 0
        
        network['timestamp'] = datetime.now(timezone.utc).isoformat()
        
        logger.info(f"Built agent network for {area}: {len(network['nodes'])} agents, {len(network['edges'])} influence edges")
        
        return network
        
    except Exception as e:
        logger.error(f"Error building agent network: {str(e)}", exc_info=True)
        return {'error': 'build_failed', 'message': str(e)}


def predict_cascade(network: dict, initiating_agent: str, initial_magnitude: float, scenario: dict = None) -> dict:
    """
    Predict cascade effects when an agent makes a price move
    
    Args:
        network: Agent network from build_agent_network()
        initiating_agent: Agent making the initial move
        initial_magnitude: Price change percentage (negative for drop)
        scenario: Optional dict with additional context {
            'market_stress': bool,
            'macro_event': str,
            'inventory_pressure': float
        }
    
    Returns: Cascade prediction with waves, probabilities, timing
    """
    try:
        if network.get('error'):
            return {'error': 'invalid_network', 'message': network.get('error')}
        
        if initiating_agent not in network['nodes']:
            return {'error': 'agent_not_found', 'message': f"{initiating_agent} not in network"}
        
        scenario = scenario or {}
        
        cascade = {
            'initiating_agent': initiating_agent,
            'initial_magnitude': initial_magnitude,
            'scenario': scenario,
            'waves': [],
            'total_affected_agents': 1,  # Include initiator
            'cascade_probability': 0,
            'expected_duration_days': 0,
            'market_impact': 'minimal'
        }
        
        affected_agents = set([initiating_agent])
        
        # WAVE 1: Direct responders to initiating agent
        wave_1 = []
        for edge_key, edge_data in network['edges'].items():
            if edge_data['from_agent'] == initiating_agent:
                to_agent = edge_data['to_agent']
                probability = edge_data['response_probability']
                
                # Adjust probability based on scenario
                adjusted_probability = probability
                if scenario.get('market_stress'):
                    adjusted_probability *= 1.2  # Higher likelihood during stress
                
                # Only include high-probability responses (>40%)
                if adjusted_probability >= 0.4:
                    magnitude_ratio = edge_data['avg_magnitude_ratio']
                    predicted_magnitude = initial_magnitude * magnitude_ratio
                    
                    avg_days = edge_data['avg_response_days']
                    days_range = edge_data['response_days_range']
                    
                    wave_1.append({
                        'agent': to_agent,
                        'wave': 1,
                        'probability': min(round(adjusted_probability, 2), 0.99),
                        'predicted_magnitude': round(predicted_magnitude, 1),
                        'timing_days': f"{int(avg_days - 2)}-{int(avg_days + 2)}",
                        'timing_avg': round(avg_days, 1),
                        'trigger': initiating_agent,
                        'confidence': 'high' if probability >= 0.7 else 'medium'
                    })
                    affected_agents.add(to_agent)
        
        if wave_1:
            cascade['waves'].append({
                'wave_number': 1,
                'agents': wave_1,
                'agent_count': len(wave_1)
            })
        
        # WAVE 2: Secondary responses (agents responding to Wave 1)
        wave_2 = []
        for wave_1_agent_data in wave_1:
            wave_1_agent = wave_1_agent_data['agent']
            wave_1_magnitude = wave_1_agent_data['predicted_magnitude']
            
            for edge_key, edge_data in network['edges'].items():
                if edge_data['from_agent'] == wave_1_agent:
                    to_agent = edge_data['to_agent']
                    
                    if to_agent in affected_agents:
                        continue  # Already responded in Wave 1
                    
                    probability = edge_data['response_probability']
                    
                    # Lower threshold for Wave 2 (compound effects)
                    if probability >= 0.35:
                        magnitude_ratio = edge_data['avg_magnitude_ratio']
                        predicted_magnitude = wave_1_magnitude * magnitude_ratio
                        
                        avg_days = edge_data['avg_response_days']
                        # Add Wave 1 timing to get total delay
                        total_days = wave_1_agent_data['timing_avg'] + avg_days
                        
                        wave_2.append({
                            'agent': to_agent,
                            'wave': 2,
                            'probability': round(probability * 0.85, 2),  # Discount for 2nd order
                            'predicted_magnitude': round(predicted_magnitude, 1),
                            'timing_days': f"{int(total_days - 3)}-{int(total_days + 5)}",
                            'timing_avg': round(total_days, 1),
                            'trigger': wave_1_agent,
                            'confidence': 'medium' if probability >= 0.5 else 'low'
                        })
                        affected_agents.add(to_agent)
        
        if wave_2:
            cascade['waves'].append({
                'wave_number': 2,
                'agents': wave_2,
                'agent_count': len(wave_2)
            })
        
        # WAVE 3: Tertiary responses (if significant Wave 2)
        if len(wave_2) >= 2:
            wave_3 = []
            for wave_2_agent_data in wave_2[:3]:  # Limit to top 3 Wave 2 agents
                wave_2_agent = wave_2_agent_data['agent']
                wave_2_magnitude = wave_2_agent_data['predicted_magnitude']
                
                for edge_key, edge_data in network['edges'].items():
                    if edge_data['from_agent'] == wave_2_agent:
                        to_agent = edge_data['to_agent']
                        
                        if to_agent in affected_agents:
                            continue
                        
                        probability = edge_data['response_probability']
                        
                        if probability >= 0.3:
                            magnitude_ratio = edge_data['avg_magnitude_ratio']
                            predicted_magnitude = wave_2_magnitude * magnitude_ratio
                            
                            avg_days = edge_data['avg_response_days']
                            total_days = wave_2_agent_data['timing_avg'] + avg_days
                            
                            wave_3.append({
                                'agent': to_agent,
                                'wave': 3,
                                'probability': round(probability * 0.7, 2),
                                'predicted_magnitude': round(predicted_magnitude, 1),
                                'timing_days': f"{int(total_days - 5)}-{int(total_days + 10)}",
                                'timing_avg': round(total_days, 1),
                                'trigger': wave_2_agent,
                                'confidence': 'low'
                            })
                            affected_agents.add(to_agent)
            
            if wave_3:
                cascade['waves'].append({
                    'wave_number': 3,
                    'agents': wave_3,
                    'agent_count': len(wave_3)
                })
        
        # CALCULATE CASCADE METRICS
        cascade['total_affected_agents'] = len(affected_agents)
        
        # Cascade probability = avg of all Wave 1 probabilities (most reliable)
        if wave_1:
            cascade['cascade_probability'] = round(
                sum([a['probability'] for a in wave_1]) / len(wave_1),
                2
            )
        else:
            cascade['cascade_probability'] = 0.0
        
        # Expected duration = max timing from all waves
        all_timings = []
        for wave in cascade['waves']:
            all_timings.extend([a['timing_avg'] for a in wave['agents']])
        
        cascade['expected_duration_days'] = round(max(all_timings)) if all_timings else 0
        
        # Market impact assessment
        total_agents_in_market = len(network['nodes'])
        affected_pct = (len(affected_agents) / total_agents_in_market) * 100
        
        if affected_pct >= 60 or cascade['cascade_probability'] >= 0.75:
            cascade['market_impact'] = 'severe'
        elif affected_pct >= 40 or cascade['cascade_probability'] >= 0.6:
            cascade['market_impact'] = 'major'
        elif affected_pct >= 20 or cascade['cascade_probability'] >= 0.4:
            cascade['market_impact'] = 'moderate'
        else:
            cascade['market_impact'] = 'minimal'
        
        cascade['timestamp'] = datetime.now(timezone.utc).isoformat()
        
        logger.info(f"Predicted cascade from {initiating_agent}: {len(affected_agents)} agents affected, {cascade['cascade_probability']*100:.0f}% probability")
        
        return cascade
        
    except Exception as e:
        logger.error(f"Error predicting cascade: {str(e)}", exc_info=True)
        return {'error': 'prediction_failed', 'message': str(e)}


def get_cascade_summary(cascade: dict) -> str:
    """
    Generate executive summary of cascade prediction
    """
    if cascade.get('error'):
        return "Cascade prediction unavailable—insufficient historical data."
    
    initiator = cascade['initiating_agent']
    magnitude = cascade['initial_magnitude']
    probability = cascade['cascade_probability'] * 100
    duration = cascade['expected_duration_days']
    impact = cascade['market_impact']
    
    wave_count = len(cascade['waves'])
    total_affected = cascade['total_affected_agents']
    
    summary_parts = [
        f"CASCADE PREDICTION: {initiator} {'+' if magnitude > 0 else ''}{magnitude:.1f}%",
        "",
        f"Probability: {probability:.0f}%",
        f"Duration: {duration:.0f} days",
        f"Impact: {impact.upper()}",
        f"Affected agents: {total_affected}",
        ""
    ]
    
    for wave_data in cascade['waves']:
        wave_num = wave_data['wave_number']
        agents = wave_data['agents']
        
        summary_parts.append(f"WAVE {wave_num}:")
        for agent in agents[:3]:  # Show top 3 per wave
            summary_parts.append(
                f"• {agent['agent']}: {agent['predicted_magnitude']:+.1f}% in {agent['timing_days']} days ({agent['probability']*100:.0f}% confidence)"
            )
        
        if len(agents) > 3:
            summary_parts.append(f"  + {len(agents) - 3} more agents...")
        summary_parts.append("")
    
    return "\n".join(summary_parts)


def store_cascade_prediction(cascade: dict):
    """
    Store cascade prediction in MongoDB for future validation
    """
    try:
        if not mongo_client or cascade.get('error'):
            return
        
        db = mongo_client['Voxmill']
        cascade_predictions = db['cascade_predictions']
        
        cascade_predictions.insert_one(cascade)
        logger.info(f"Stored cascade prediction for {cascade['initiating_agent']}")
        
    except Exception as e:
        logger.error(f"Error storing cascade prediction: {str(e)}", exc_info=True)
