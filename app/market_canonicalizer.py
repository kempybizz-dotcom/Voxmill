"""
CANONICAL MARKET RESOLVER
=========================
Maps user phrases to canonical market IDs BEFORE dataset loading
Prevents "The Entire London Area" from being treated as a literal market
"""

import logging

logger = logging.getLogger(__name__)


class MarketCanonicalizer:
    """Resolve user market phrases to canonical IDs"""
    
    # Canonical alias table
    CANONICAL_ALIASES = {
        # London variants
        "london": "LONDON_GENERAL",
        "greater london": "LONDON_GENERAL",
        "entire london": "LONDON_GENERAL",
        "entire london area": "LONDON_GENERAL",
        "the entire london area": "LONDON_GENERAL",
        "london overall": "LONDON_GENERAL",
        "all of london": "LONDON_GENERAL",
        "london wide": "LONDON_GENERAL",
        
        # Keep existing specific markets as-is
        "mayfair": "Mayfair",
        "knightsbridge": "Knightsbridge",
        "chelsea": "Chelsea",
        "belgravia": "Belgravia",
        "kensington": "Kensington",
        "notting hill": "Notting Hill",
        "south kensington": "South Kensington",
        
        # Manchester variants (for structural comparison)
        "manchester": "MANCHESTER_GENERAL",
        "greater manchester": "MANCHESTER_GENERAL",
        
        # Birmingham variants
        "birmingham": "BIRMINGHAM_GENERAL",
        "greater birmingham": "BIRMINGHAM_GENERAL",
    }
    
    # Markets that should use structural comparison (no dataset)
    STRUCTURAL_ONLY_MARKETS = {
        "LONDON_GENERAL",
        "MANCHESTER_GENERAL",
        "BIRMINGHAM_GENERAL",
    }
    
    @classmethod
    def canonicalize(cls, market_name: str) -> tuple[str, bool]:
        """
        Resolve market name to canonical ID
        
        Returns:
            (canonical_id, is_structural_only)
        """
        
        if not market_name:
            return market_name, False
        
        # Normalize for lookup
        normalized = market_name.strip().lower()
        
        # Check alias table
        canonical = cls.CANONICAL_ALIASES.get(normalized, market_name)
        
        # Check if structural-only
        is_structural = canonical in cls.STRUCTURAL_ONLY_MARKETS
        
        if canonical != market_name:
            logger.info(f"🔄 Market canonicalized: '{market_name}' → '{canonical}' (structural={is_structural})")
        
        return canonical, is_structural
    
    @classmethod
    def is_structural_market(cls, market_name: str) -> bool:
        """Check if market should use structural comparison only"""
        canonical, _ = cls.canonicalize(market_name)
        return canonical in cls.STRUCTURAL_ONLY_MARKETS
