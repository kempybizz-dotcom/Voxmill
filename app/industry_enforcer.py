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
        """
        Get industry-specific vocabulary mapping
        
        Args:
            industry: Lowercase industry code (e.g., 'real_estate', 'hedge_fund')
        
        Returns:
            Vocabulary mapping dict
        """
        # Map lowercase codes to enum
        industry_code_map = {
            'real_estate': Industry.REAL_ESTATE,
            'luxury_assets': Industry.AUTOMOTIVE,
            'automotive': Industry.AUTOMOTIVE,
            'healthcare': Industry.HEALTHCARE,
            'hospitality': Industry.HOSPITALITY,
            'luxury_retail': Industry.LUXURY_RETAIL,
            'private_equity': Industry.PRIVATE_EQUITY,
            'venture_capital': Industry.VENTURE_CAPITAL,
            'yachting': Industry.YACHTING,
            'aviation': Industry.AVIATION
        }
        
        try:
            industry_enum = industry_code_map.get(industry.lower())
            
            if industry_enum:
                return IndustryEnforcer.INDUSTRY_VOCABULARY.get(
                    industry_enum,
                    IndustryEnforcer.INDUSTRY_VOCABULARY[Industry.REAL_ESTATE]
                )
            else:
                logger.warning(f"Unknown industry code: {industry}, defaulting to Real Estate")
                return IndustryEnforcer.INDUSTRY_VOCABULARY[Industry.REAL_ESTATE]
        
        except Exception as e:
            logger.error(f"Get vocabulary error: {e}")
            return IndustryEnforcer.INDUSTRY_VOCABULARY[Industry.REAL_ESTATE]
    
    @staticmethod
    def get_supported_regions(industry_code: str) -> List[str]:
        """
        Query Markets table for regions supported by this industry
        
        ✅ DYNAMIC - Queries Airtable, NO hardcoded lists
        
        Args:
            industry_code: Lowercase industry code (e.g., 'real_estate', 'hedge_fund')
        
        Returns:
            List of market names or empty list if none configured
        """
        import os
        import requests
        
        AIRTABLE_API_KEY = os.getenv('AIRTABLE_API_KEY')
        AIRTABLE_BASE_ID = os.getenv('AIRTABLE_BASE_ID')
        
        if not AIRTABLE_API_KEY or not AIRTABLE_BASE_ID:
            logger.warning("Airtable not configured")
            return []
        
        try:
            headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
            url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/Markets"
            
            # Query for active, selectable markets in this industry
            formula = f"AND({{industry}}='{industry_code}', {{is_active}}=TRUE(), {{Selectable}}=TRUE())"
            params = {'filterByFormula': formula}
            
            response = requests.get(url, headers=headers, params=params, timeout=5)
            
            if response.status_code == 200:
                records = response.json().get('records', [])
                markets = [r['fields'].get('market_name') for r in records if r['fields'].get('market_name')]
                
                if not markets:
                    logger.warning(f"⚠️ NO MARKETS CONFIGURED in Airtable for industry: {industry_code}")
                
                return markets  # Returns [] if no markets configured
            
            logger.error(f"Markets table query failed: {response.status_code}")
            return []
        
        except Exception as e:
            logger.error(f"Get supported regions failed: {e}")
            return []
    
    @staticmethod
    def is_supported(industry_code: str, region: str) -> bool:
        """
        Check if region is supported for industry
        
        ✅ DYNAMIC - Queries Airtable Markets table
        
        Args:
            industry_code: Lowercase industry code (e.g., 'real_estate', 'hedge_fund')
            region: Market name (e.g., 'Mayfair', 'Manhattan')
        
        Returns:
            True if market is active and selectable for this industry
        """
        supported_regions = IndustryEnforcer.get_supported_regions(industry_code)
        return region in supported_regions
    
    @staticmethod
    def get_metric_name(industry: str, metric_key: str) -> str:
        """
        Get industry-specific metric display name
        
        Args:
            industry: Lowercase industry code (e.g., 'real_estate', 'hedge_fund')
            metric_key: Metric key (e.g., 'avg_price', 'days_on_market')
        
        Returns:
            Display name for metric
        """
        # Map lowercase codes to enum
        industry_code_map = {
            'real_estate': Industry.REAL_ESTATE,
            'luxury_assets': Industry.AUTOMOTIVE,
            'automotive': Industry.AUTOMOTIVE,
            'healthcare': Industry.HEALTHCARE,
            'hospitality': Industry.HOSPITALITY,
            'luxury_retail': Industry.LUXURY_RETAIL,
            'private_equity': Industry.PRIVATE_EQUITY,
            'venture_capital': Industry.VENTURE_CAPITAL,
            'yachting': Industry.YACHTING,
            'aviation': Industry.AVIATION
        }
        
        try:
            industry_enum = industry_code_map.get(industry.lower())
            
            if industry_enum:
                metrics = IndustryEnforcer.INDUSTRY_METRICS.get(
                    industry_enum,
                    IndustryEnforcer.INDUSTRY_METRICS[Industry.REAL_ESTATE]
                )
                return metrics.get(metric_key, metric_key.replace('_', ' ').title())
            else:
                return metric_key.replace('_', ' ').title()
        
        except Exception as e:
            logger.error(f"Get metric name error: {e}")
            return metric_key.replace('_', ' ').title()
    
    @staticmethod
    def get_industry_context(industry_code: str) -> str:
        """
        Get industry-specific context for LLM system prompt
        
        Args:
            industry_code: Lowercase industry code (e.g., 'real_estate', 'hedge_fund')
        
        Returns:
            Industry context string for LLM
        """
        
        # Map lowercase codes to enum
        industry_code_map = {
            'real_estate': Industry.REAL_ESTATE,
            'luxury_assets': Industry.AUTOMOTIVE,
            'automotive': Industry.AUTOMOTIVE,
            'healthcare': Industry.HEALTHCARE,
            'hospitality': Industry.HOSPITALITY,
            'luxury_retail': Industry.LUXURY_RETAIL,
            'private_equity': Industry.PRIVATE_EQUITY,
            'venture_capital': Industry.VENTURE_CAPITAL,
            'yachting': Industry.YACHTING,
            'aviation': Industry.AVIATION
        }
        
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
""",
            
            Industry.PRIVATE_EQUITY: """
MARKET CONTEXT: Private equity investments
ENTITIES: PE firms, portfolio companies
VOCABULARY: Deals, valuations, exit strategies, IRR
METRICS: Deal flow, fund performance, sector positioning
""",
            
            Industry.YACHTING: """
MARKET CONTEXT: Superyacht sales and charter
ENTITIES: Yacht brokers, shipyards
VOCABULARY: Vessels, listings, charter rates, specifications
METRICS: Days on market, pricing trends, broker positioning
"""
        }
        
        try:
            industry_enum = industry_code_map.get(industry_code.lower())
            
            if industry_enum and industry_enum in industry_contexts:
                return industry_contexts[industry_enum]
            else:
                # Default to Real Estate
                return industry_contexts[Industry.REAL_ESTATE]
        
        except Exception as e:
            logger.error(f"Get industry context error: {e}")
            return industry_contexts[Industry.REAL_ESTATE]
    
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
