"""
VOXMILL SCORING ENGINE
Centralized intelligence calculations with vertical-specific thresholds
"""

class VoxmillScoringEngine:
    """Pure functions for all scoring logic"""
    
    # Vertical-specific thresholds
    THRESHOLDS = {
        'real_estate_mayfair': {
            'velocity_excellent': 30,
            'velocity_good': 45,
            'velocity_fair': 60,
            'price_premium_threshold': 2000,
            'liquidity_bands': [60, 75, 85]
        },
        'real_estate_miami': {
            'velocity_excellent': 25,
            'velocity_good': 40,
            # ...
        }
    }
    
    @staticmethod
    def absorption_rate_score(days: int, vertical: str = 'real_estate_mayfair') -> int:
        """
        Calculate velocity score (0-100 scale)
        
        Args:
            days: Days on market
            vertical: Vertical identifier for thresholds
            
        Returns:
            Score 0-100 (higher = faster absorption)
        """
        config = VoxmillScoringEngine.THRESHOLDS.get(
            vertical,
            VoxmillScoringEngine.THRESHOLDS['real_estate_mayfair']
        )
        
        if days <= config['velocity_excellent']:
            return 95  # Exceptional
        elif days <= config['velocity_good']:
            return 80  # Strong
        elif days <= config['velocity_fair']:
            return 65  # Moderate
        else:
            # Linear decay after fair threshold
            decay = min((days - config['velocity_fair']) / 2, 40)
            return max(55, int(65 - decay))
    
    @staticmethod
    def liquidity_index(volume: int, days: int, vertical: str) -> int:
        """Calculate market liquidity index (0-100)"""
        base_score = 100 - (days / 2)
        volume_multiplier = min(volume / 50, 1.2)
        return max(0, min(100, int(base_score * volume_multiplier)))
    
    @staticmethod
    def demand_pressure_index(active_inventory: int, recent_sales: int) -> float:
        """Calculate supply/demand ratio"""
        if recent_sales == 0:
            return 2.0  # High oversupply
        return round(active_inventory / recent_sales, 2)
    
    @staticmethod
    def classify_velocity_signal(score: int) -> str:
        """Convert score to human label"""
        if score >= 85:
            return "Strong Demand"
        elif score >= 65:
            return "Moderate"
        elif score >= 50:
            return "Weak"
        else:
            return "Stagnant"
    
    @staticmethod
    def classify_badge_style(score: int) -> str:
        """Return CSS class for badge coloring"""
        if score >= 85:
            return "vx-bullish"
        elif score >= 65:
            return "vx-neutral"
        else:
            return "vx-cautious"
