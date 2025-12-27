"""
VOXMILL INDUSTRY ENFORCEMENT
=============================
Routes dataset loading and enforces industry-specific vocabulary

Supported Industries:
- Real Estate
- Automotive
- Healthcare (Medical Aesthetics)
- Hospitality
- Luxury Retail

CRITICAL: Only load datasets and use vocabulary appropriate for client's industry
"""

import logging
from typing import Dict, List, Optional
from enum import Enum

logger = logging.getLogger(__name__)


class Industry(Enum):
    """Supported industries"""
    REAL_ESTATE = "Real Estate"
    AUTOMOTIVE = "Automotive"
    HEALTHCARE = "Healthcare"
    HOSPITALITY = "Hospitality"
    LUXURY_RETAIL = "Luxury Retail"
    PRIVATE_EQUITY = "Private Equity"
    VENTURE_CAPITAL = "Venture Capital"
    YACHTING = "Yachting"
    AVIATION = "Aviation"


class IndustryEnforcer:
    """Enforces industry-specific constraints"""
    
    # ========================================
    # INDUSTRY → VOCABULARY MAPPING
    # ========================================
    
    INDUSTRY_VOCABULARY = {
        Industry.REAL_ESTATE: {
            'entity': 'agent',
            'entities': 'agents',
            'listing': 'property',
            'listings': 'properties',
            'inventory': 'listings',
            'price': 'asking price',
            'market': 'property market',
            'competitors': 'competing agents',
            'segment': 'property type',
            'segments': 'property types',
            'client': 'buyer',
            'clients': 'buyers'
        },
        
        Industry.AUTOMOTIVE: {
            'entity': 'dealership',
            'entities': 'dealerships',
            'listing': 'vehicle',
            'listings': 'inventory',
            'inventory': 'stock',
            'price': 'sticker price',
            'market': 'automotive market',
            'competitors': 'competing dealerships',
            'segment': 'vehicle class',
            'segments': 'vehicle classes',
            'client': 'customer',
            'clients': 'customers'
        },
        
        Industry.HEALTHCARE: {
            'entity': 'clinic',
            'entities': 'clinics',
            'listing': 'treatment',
            'listings': 'services',
            'inventory': 'treatment menu',
            'price': 'treatment price',
            'market': 'aesthetics market',
            'competitors': 'competing practices',
            'segment': 'treatment category',
            'segments': 'treatment categories',
            'client': 'patient',
            'clients': 'patients'
        },
        
        Industry.HOSPITALITY: {
            'entity': 'hotel',
            'entities': 'hotels',
            'listing': 'room',
            'listings': 'availability',
            'inventory': 'occupancy',
            'price': 'room rate',
            'market': 'hospitality market',
            'competitors': 'competing hotels',
            'segment': 'room class',
            'segments': 'room classes',
            'client': 'guest',
            'clients': 'guests'
        },
        
        Industry.LUXURY_RETAIL: {
            'entity': 'boutique',
            'entities': 'boutiques',
            'listing': 'product',
            'listings': 'collection',
            'inventory': 'stock',
            'price': 'retail price',
            'market': 'luxury market',
            'competitors': 'competing brands',
            'segment': 'product category',
            'segments': 'product categories',
            'client': 'customer',
            'clients': 'customers'
        }
    }
    
    # ========================================
    # INDUSTRY → SUPPORTED REGIONS
    # ========================================
    
    INDUSTRY_REGIONS = {
        Industry.REAL_ESTATE: [
            'Mayfair', 'Knightsbridge', 'Chelsea', 'Belgravia', 'Kensington',
            'South Kensington', 'Notting Hill', 'Marylebone'
        ],
        
        Industry.AUTOMOTIVE: [
            'London Central', 'Mayfair', 'Kensington', 'Chelsea'
        ],
        
        Industry.HEALTHCARE: [
            'Harley Street', 'Mayfair', 'Kensington', 'Chelsea'
        ],
        
        Industry.HOSPITALITY: [
            'Mayfair', 'Knightsbridge', 'Belgravia', 'Covent Garden'
        ],
        
        Industry.LUXURY_RETAIL: [
            'Bond Street', 'Sloane Street', 'Kings Road', 'Mayfair'
        ]
    }
    
    # ========================================
    # INDUSTRY → METRIC NAMES
    # ========================================
    
    INDUSTRY_METRICS = {
        Industry.REAL_ESTATE: {
            'avg_price': 'Average Asking Price',
            'median_price': 'Median Price',
            'price_per_sqft': 'Price per Sq Ft',
            'inventory_count': 'Active Listings',
            'days_on_market': 'Days on Market'
        },
        
        Industry.AUTOMOTIVE: {
            'avg_price': 'Average List Price',
            'median_price': 'Median Price',
            'price_per_sqft': 'N/A',
            'inventory_count': 'Available Units',
            'days_on_market': 'Days in Stock'
        },
        
        Industry.HEALTHCARE: {
            'avg_price': 'Average Treatment Price',
            'median_price': 'Median Price Point',
            'price_per_sqft': 'N/A',
            'inventory_count': 'Active Treatments',
            'days_on_market': 'N/A'
        }
    }
    
    @staticmethod
    def get_vocabulary(industry: str) -> Dict[str, str]:
        """Get industry-specific vocabulary mapping"""
        try:
            industry_enum = Industry(industry)
            return IndustryEnforcer.INDUSTRY_VOCABULARY.get(
                industry_enum,
                IndustryEnforcer.INDUSTRY_VOCABULARY[Industry.REAL_ESTATE]
            )
        except ValueError:
            logger.warning(f"Unknown industry: {industry}, defaulting to Real Estate")
            return IndustryEnforcer.INDUSTRY_VOCABULARY[Industry.REAL_ESTATE]
    
    @staticmethod
    def get_supported_regions(industry: str) -> List[str]:
        """Get regions supported for this industry"""
        try:
            industry_enum = Industry(industry)
            return IndustryEnforcer.INDUSTRY_REGIONS.get(
                industry_enum,
                IndustryEnforcer.INDUSTRY_REGIONS[Industry.REAL_ESTATE]
            )
        except ValueError:
            return IndustryEnforcer.INDUSTRY_REGIONS[Industry.REAL_ESTATE]
    
    @staticmethod
    def is_supported(industry: str, region: str) -> bool:
        """Check if region is supported for industry"""
        supported_regions = IndustryEnforcer.get_supported_regions(industry)
        return region in supported_regions
    
    @staticmethod
    def get_metric_name(industry: str, metric_key: str) -> str:
        """Get industry-specific metric display name"""
        try:
            industry_enum = Industry(industry)
            metrics = IndustryEnforcer.INDUSTRY_METRICS.get(
                industry_enum,
                IndustryEnforcer.INDUSTRY_METRICS[Industry.REAL_ESTATE]
            )
            return metrics.get(metric_key, metric_key.replace('_', ' ').title())
        except ValueError:
            return metric_key.replace('_', ' ').title()
    
    @staticmethod
    def apply_vocabulary_to_prompt(prompt: str, industry: str) -> str:
        """
        Apply industry-specific vocabulary to LLM prompt
        
        Replaces generic terms with industry-appropriate language
        """
        vocabulary = IndustryEnforcer.get_vocabulary(industry)
        
        # Apply replacements (case-insensitive)
        modified_prompt = prompt
        
        for generic_term, industry_term in vocabulary.items():
            # Replace whole words only (avoid partial matches)
            import re
            pattern = r'\b' + re.escape(generic_term) + r'\b'
            modified_prompt = re.sub(
                pattern,
                industry_term,
                modified_prompt,
                flags=re.IGNORECASE
            )
        
        return modified_prompt
    
    @staticmethod
    def get_industry_context(industry: str) -> str:
        """Get industry-specific context for LLM system prompt"""
        
        industry_contexts = {
            Industry.REAL_ESTATE: """
MARKET CONTEXT: Luxury residential property
ENTITIES: Estate agents (Knight Frank, Savills, Hamptons, etc.)
VOCABULARY: Properties, listings, asking prices, £/sqft
METRICS: Days on market, inventory velocity, agent positioning
""",
            
            Industry.AUTOMOTIVE: """
MARKET CONTEXT: Premium automotive retail
ENTITIES: Dealerships (official brand dealers, premium used)
VOCABULARY: Vehicles, inventory, sticker prices, model variants
METRICS: Days in stock, inventory turnover, dealership positioning
""",
            
            Industry.HEALTHCARE: """
MARKET CONTEXT: Medical aesthetics and premium clinics
ENTITIES: Private clinics, practitioners
VOCABULARY: Treatments, services, treatment prices, practitioner profiles
METRICS: Treatment popularity, pricing tiers, clinic positioning
""",
            
            Industry.HOSPITALITY: """
MARKET CONTEXT: Luxury hospitality
ENTITIES: Hotels, boutique properties
VOCABULARY: Rooms, occupancy, room rates, guest experience
METRICS: Occupancy rates, ADR, RevPAR, competitive positioning
""",
            
            Industry.LUXURY_RETAIL: """
MARKET CONTEXT: Luxury retail
ENTITIES: Boutiques, flagship stores
VOCABULARY: Products, collections, retail prices, brand positioning
METRICS: Stock levels, pricing strategy, foot traffic
"""
        }
        
        try:
            industry_enum = Industry(industry)
            return industry_contexts.get(
                industry_enum,
                industry_contexts[Industry.REAL_ESTATE]
            )
        except ValueError:
            return industry_contexts[Industry.REAL_ESTATE]


def route_dataset_loader(industry: str, area: str, max_results: int = 100) -> Dict:
    """
    Route to appropriate dataset loader based on industry
    
    Args:
        industry: Industry name (e.g., "Real Estate", "Automotive")
        area: Geographic area
        max_results: Maximum items to fetch
    
    Returns:
        Dataset dict or empty dataset if industry not supported
    """
    
    logger.info(f"Routing dataset loader: industry={industry}, area={area}")
    
    # Check if region is supported for this industry
    if not IndustryEnforcer.is_supported(industry, area):
        logger.warning(f"Region '{area}' not supported for industry '{industry}'")
        from app.dataset_loader import _empty_dataset
        return _empty_dataset(area)
    
    # Route to industry-specific loader
    if industry == "Real Estate":
        from app.dataset_loader import load_dataset as load_real_estate_dataset
        return load_real_estate_dataset(area, max_results)
    
    elif industry == "Automotive":
        try:
            from app.dataset_loader_automotive import load_automotive_dataset
            return load_automotive_dataset(area, max_results)
        except ImportError:
            logger.error("Automotive dataset loader not available")
            from app.dataset_loader import _empty_dataset
            return _empty_dataset(area)
    
    elif industry == "Healthcare":
        try:
            from app.dataset_loader_healthcare import load_healthcare_dataset
            return load_healthcare_dataset(area, max_results)
        except ImportError:
            logger.error("Healthcare dataset loader not available")
            from app.dataset_loader import _empty_dataset
            return _empty_dataset(area)
    
    elif industry == "Hospitality":
        logger.warning("Hospitality dataset loader not yet implemented")
        from app.dataset_loader import _empty_dataset
        return _empty_dataset(area)
    
    elif industry == "Luxury Retail":
        logger.warning("Luxury Retail dataset loader not yet implemented")
        from app.dataset_loader import _empty_dataset
        return _empty_dataset(area)
    
    else:
        logger.warning(f"Unknown industry: {industry}, defaulting to Real Estate")
        from app.dataset_loader import load_dataset as load_real_estate_dataset
        return load_real_estate_dataset(area, max_results)
