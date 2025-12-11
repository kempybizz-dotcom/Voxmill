"""
VOXMILL LIQUIDITY WINDOW PREDICTOR
===================================
Predicts optimal buy/sell windows based on historical velocity patterns

Institutional Use Case:
"When should we enter this market?" → "14-21 day window opening, 73% confidence"
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional
import statistics

logger = logging.getLogger(__name__)


def predict_liquidity_windows(area: str, current_velocity: Dict, historical_data: List[Dict]) -> Dict:
    """
    Predict upcoming liquidity windows based on velocity patterns
    
    Args:
        area: Market area
        current_velocity: Current liquidity metrics
        historical_data: List of historical velocity snapshots
    
    Returns: Dict with window predictions
    """
    
    try:
        if not historical_data or len(historical_data) < 10:
            return {
                'error': 'insufficient_data',
                'message': 'Need 10+ historical snapshots for prediction'
            }
        
        # Extract velocity scores over time
        velocity_series = []
        for snapshot in historical_data[-30:]:  # Last 30 data points
            velocity = snapshot.get('liquidity_velocity', {})
            if velocity and not velocity.get('error'):
                velocity_series.append({
                    'score': velocity.get('velocity_score', 50),
                    'timestamp': snapshot.get('metadata', {}).get('analysis_timestamp')
                })
        
        if len(velocity_series) < 10:
            return {
                'error': 'insufficient_velocity_data',
                'message': 'Need 10+ velocity measurements'
            }
        
        # Calculate velocity trends
        scores = [v['score'] for v in velocity_series]
        
        current_score = current_velocity.get('velocity_score', 50)
        avg_score = statistics.mean(scores)
        std_dev = statistics.stdev(scores) if len(scores) > 1 else 0
        
        # Detect momentum
        recent_scores = scores[-5:]
        older_scores = scores[-10:-5] if len(scores) >= 10 else scores[:5]
        
        recent_avg = statistics.mean(recent_scores)
        older_avg = statistics.mean(older_scores)
        
        momentum = ((recent_avg - older_avg) / older_avg * 100) if older_avg > 0 else 0
        
        # Detect cyclical patterns
        cycle_analysis = detect_velocity_cycles(scores)
        
        # Predict windows
        windows = []
        
        # WINDOW TYPE 1: HIGH LIQUIDITY (Seller's market)
        if current_score >= 70:
            windows.append({
                'type': 'high_liquidity',
                'status': 'active',
                'confidence': 0.85,
                'timing': 'now',
                'duration_days': 14,
                'recommendation': 'SELL',
                'rationale': f'Velocity at {current_score}/100 (Strong). Optimal exit window.',
                'urgency': 'immediate'
            })
        
        elif current_score >= 60 and momentum > 5:
            windows.append({
                'type': 'high_liquidity',
                'status': 'approaching',
                'confidence': 0.72,
                'timing': '7-14 days',
                'duration_days': 14,
                'recommendation': 'PREPARE_SELL',
                'rationale': f'Velocity rising ({momentum:+.1f}%). Peak window ahead.',
                'urgency': 'high'
            })
        
        # WINDOW TYPE 2: LOW LIQUIDITY (Buyer's market)
        if current_score <= 30:
            windows.append({
                'type': 'low_liquidity',
                'status': 'active',
                'confidence': 0.88,
                'timing': 'now',
                'duration_days': 21,
                'recommendation': 'BUY',
                'rationale': f'Velocity at {current_score}/100 (Weak). Buyer leverage maximized.',
                'urgency': 'immediate'
            })
        
        elif current_score <= 40 and momentum < -5:
            windows.append({
                'type': 'low_liquidity',
                'status': 'approaching',
                'confidence': 0.75,
                'timing': '7-14 days',
                'duration_days': 21,
                'recommendation': 'PREPARE_BUY',
                'rationale': f'Velocity declining ({momentum:+.1f}%). Entry window opening.',
                'urgency': 'high'
            })
        
        # WINDOW TYPE 3: EQUILIBRIUM (Wait)
        if 40 < current_score < 60 and abs(momentum) < 5:
            windows.append({
                'type': 'equilibrium',
                'status': 'active',
                'confidence': 0.65,
                'timing': 'now',
                'duration_days': 30,
                'recommendation': 'HOLD',
                'rationale': f'Velocity stable at {current_score}/100. No tactical advantage.',
                'urgency': 'low'
            })
        
        # WINDOW TYPE 4: REVERSAL PREDICTION
        if cycle_analysis.get('pattern_detected'):
            next_peak = cycle_analysis.get('next_peak_days')
            next_trough = cycle_analysis.get('next_trough_days')
            
            if next_peak and next_peak < 30:
                windows.append({
                    'type': 'reversal_peak',
                    'status': 'predicted',
                    'confidence': cycle_analysis.get('confidence', 0.6),
                    'timing': f'{next_peak} days',
                    'duration_days': 7,
                    'recommendation': 'SELL',
                    'rationale': f'Cyclical peak predicted in {next_peak} days (pattern confidence: {cycle_analysis["confidence"]*100:.0f}%)',
                    'urgency': 'medium'
                })
            
            if next_trough and next_trough < 30:
                windows.append({
                    'type': 'reversal_trough',
                    'status': 'predicted',
                    'confidence': cycle_analysis.get('confidence', 0.6),
                    'timing': f'{next_trough} days',
                    'duration_days': 7,
                    'recommendation': 'BUY',
                    'rationale': f'Cyclical trough predicted in {next_trough} days (pattern confidence: {cycle_analysis["confidence"]*100:.0f}%)',
                    'urgency': 'medium'
                })
        
        # Sort windows by urgency and confidence
        urgency_order = {'immediate': 0, 'high': 1, 'medium': 2, 'low': 3}
        windows.sort(key=lambda w: (urgency_order.get(w['urgency'], 4), -w['confidence']))
        
        # Calculate overall market timing score
        timing_score = calculate_timing_score(current_score, momentum, std_dev)
        
        return {
            'area': area,
            'current_velocity': current_score,
            'velocity_momentum': round(momentum, 2),
            'volatility': round(std_dev, 2),
            'timing_score': timing_score,
            'timing_recommendation': get_timing_recommendation(timing_score),
            'predicted_windows': windows,
            'total_windows': len(windows),
            'cycle_analysis': cycle_analysis,
            'confidence_notes': generate_confidence_notes(current_score, momentum, len(scores))
        }
        
    except Exception as e:
        logger.error(f"Error predicting liquidity windows: {e}", exc_info=True)
        return {
            'error': 'prediction_failed',
            'message': str(e)
        }


def detect_velocity_cycles(scores: List[float]) -> Dict:
    """
    Detect cyclical patterns in velocity scores
    
    Returns: Dict with cycle information
    """
    
    try:
        if len(scores) < 15:
            return {'pattern_detected': False}
        
        # Find peaks and troughs
        peaks = []
        troughs = []
        
        for i in range(1, len(scores) - 1):
            if scores[i] > scores[i-1] and scores[i] > scores[i+1]:
                peaks.append(i)
            elif scores[i] < scores[i-1] and scores[i] < scores[i+1]:
                troughs.append(i)
        
        if len(peaks) < 2 or len(troughs) < 2:
            return {'pattern_detected': False}
        
        # Calculate average cycle length
        peak_intervals = [peaks[i] - peaks[i-1] for i in range(1, len(peaks))]
        trough_intervals = [troughs[i] - troughs[i-1] for i in range(1, len(troughs))]
        
        avg_cycle = statistics.mean(peak_intervals + trough_intervals)
        cycle_consistency = 1 - (statistics.stdev(peak_intervals + trough_intervals) / avg_cycle) if avg_cycle > 0 else 0
        
        # Predict next peak/trough
        last_peak_pos = peaks[-1] if peaks else None
        last_trough_pos = troughs[-1] if troughs else None
        
        days_since_peak = len(scores) - last_peak_pos if last_peak_pos else None
        days_since_trough = len(scores) - last_trough_pos if last_trough_pos else None
        
        next_peak_days = int(avg_cycle - days_since_peak) if days_since_peak is not None else None
        next_trough_days = int(avg_cycle - days_since_trough) if days_since_trough is not None else None
        
        # Adjust predictions if they're negative (cycle already passed)
        if next_peak_days and next_peak_days < 0:
            next_peak_days += int(avg_cycle)
        if next_trough_days and next_trough_days < 0:
            next_trough_days += int(avg_cycle)
        
        return {
            'pattern_detected': True,
            'avg_cycle_length': round(avg_cycle, 1),
            'consistency_score': round(cycle_consistency, 2),
            'confidence': min(0.85, cycle_consistency),
            'peaks_detected': len(peaks),
            'troughs_detected': len(troughs),
            'next_peak_days': next_peak_days if next_peak_days and next_peak_days > 0 else None,
            'next_trough_days': next_trough_days if next_trough_days and next_trough_days > 0 else None
        }
        
    except Exception as e:
        logger.error(f"Error detecting cycles: {e}")
        return {'pattern_detected': False, 'error': str(e)}


def calculate_timing_score(velocity: float, momentum: float, volatility: float) -> int:
    """
    Calculate overall market timing score (0-100)
    
    Higher score = Better time to act
    """
    
    score = 50  # Neutral baseline
    
    # Factor 1: Velocity extremes (30% weight)
    if velocity >= 70:
        score += 15  # High liquidity = good for sellers
    elif velocity <= 30:
        score += 15  # Low liquidity = good for buyers
    else:
        score -= 5  # Equilibrium = wait
    
    # Factor 2: Strong momentum (25% weight)
    if abs(momentum) > 10:
        score += 12
    elif abs(momentum) > 5:
        score += 6
    
    # Factor 3: Low volatility = predictability (20% weight)
    if volatility < 10:
        score += 10
    elif volatility < 20:
        score += 5
    else:
        score -= 5
    
    # Factor 4: Directional clarity (25% weight)
    if (velocity > 60 and momentum > 5) or (velocity < 40 and momentum < -5):
        score += 12  # Clear trend = good timing
    
    return max(0, min(100, score))


def get_timing_recommendation(score: int) -> str:
    """Convert timing score to recommendation"""
    
    if score >= 75:
        return 'STRONG_SIGNAL'
    elif score >= 60:
        return 'FAVORABLE'
    elif score >= 45:
        return 'NEUTRAL'
    elif score >= 30:
        return 'UNFAVORABLE'
    else:
        return 'WAIT'


def generate_confidence_notes(velocity: float, momentum: float, data_points: int) -> str:
    """Generate confidence assessment notes"""
    
    notes = []
    
    if data_points >= 30:
        notes.append("Strong historical basis (30+ data points)")
    elif data_points >= 15:
        notes.append("Adequate historical basis (15+ data points)")
    else:
        notes.append("Limited historical basis (<15 data points)")
    
    if abs(momentum) > 10:
        notes.append("Strong directional momentum detected")
    elif abs(momentum) < 3:
        notes.append("Stable/stagnant conditions")
    
    if velocity > 70 or velocity < 30:
        notes.append("Extreme velocity reading increases confidence")
    
    return " | ".join(notes)


def format_window_alert(window: Dict) -> str:
    """Format window prediction as alert message"""
    
    urgency_emoji = {
        'immediate': '🔴',
        'high': '🟠',
        'medium': '🟡',
        'low': '⚪'
    }
    
    emoji = urgency_emoji.get(window['urgency'], '⚪')
    
    message = f"""{emoji} LIQUIDITY WINDOW ALERT

TYPE: {window['type'].replace('_', ' ').title()}
STATUS: {window['status'].upper()}
TIMING: {window['timing']}
DURATION: {window['duration_days']} days
CONFIDENCE: {window['confidence']*100:.0f}%

RECOMMENDATION: {window['recommendation']}

{window['rationale']}

Urgency: {window['urgency'].upper()}"""
    
    return message
