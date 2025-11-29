#!/usr/bin/env python3
"""
VOXMILL STRESS SCENARIO GENERATOR
==================================
Produces realistic, varied datasets for testing layout resilience.
Tests the PDF engine under extreme conditions to ensure bulletproof operation.

USAGE:
    from stress_scenarios import VoxmillDataFactory
    
    factory = VoxmillDataFactory()
    data = factory.generate_scenario('stress_long_labels')
    
SCENARIOS AVAILABLE:
    • baseline_mayfair          - Normal 40-property dataset
    • stress_long_labels        - 80-120 char addresses/agencies
    • stress_extreme_values     - Prices £50k-£50M, days 0-365
    • stress_dense_tables       - 100 properties, 15 submarkets
    • stress_sparse             - 2 properties, 1 submarket
    • stress_nulls              - 30% missing data
    • stress_zero_properties    - Edge case: empty dataset
    • stress_single_property    - Edge case: 1 property only
"""

import random
import json
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional


class VoxmillDataFactory:
    """
    Enterprise-grade demo data generation for stress testing.
    Ensures PDF engine never breaks regardless of input conditions.
    """
    
    # Realistic data pools for generation
    STREET_NAMES_SHORT = [
        'Park Lane', 'Mount Street', 'Curzon Street', 'Davies Street',
        'South Street', 'Hill Street', 'Green Street', 'Duke Street'
    ]
    
    STREET_NAMES_LONG = [
        'Upper Grosvenor Street and Westminster Gardens Extension',
        'The Royal Crescent Mews and Historical Quarter Development',
        'Knightsbridge International Business and Residential District',
        'Belgravia Square Historic Preservation Area and Conservation Zone',
        'Mayfair Premium Residential Development and Luxury Quarter'
    ]
    
    PROPERTY_TYPES = [
        'Penthouse', 'Townhouse', 'Apartment', 'Duplex',
        'Mews House', 'Mansion', 'Garden Flat', 'Studio'
    ]
    
    AGENCIES_SHORT = [
        'Knight Frank', 'Savills', 'Strutt & Parker',
        'Hamptons', 'Chestertons', 'Foxtons'
    ]
    
    AGENCIES_LONG = [
        'Knight Frank International Luxury Property Consultants Limited',
        'Savills Premium Real Estate and Investment Advisory Services',
        'Strutt & Parker Historical Properties and Land Management Group',
        'Hamptons International Residential and Commercial Estate Agents',
        'Chestertons Global Property Solutions and Portfolio Management'
    ]
    
    SUBMARKETS_MAYFAIR = [
        'Mayfair', 'Mayfair North', 'Mayfair South', 'Mayfair East',
        'Grosvenor Square', 'Berkeley Square', 'Mount Street Quarter',
        'Curzon Street District', 'Park Lane Corridor', 'Davies Street Area',
        'South Audley Precinct', 'Brook Street Zone', 'Carlos Place',
        'Hertford Street', 'Chesterfield Hill'
    ]
    
    # Scenario configurations
    SCENARIOS = {
        'baseline_mayfair': {
            'properties': 40,
            'mode': 'normal',
            'vertical': 'real_estate',
            'area': 'Mayfair',
            'city': 'London'
        },
        'stress_long_labels': {
            'properties': 25,
            'mode': 'extreme_text',
            'vertical': 'real_estate',
            'area': 'Mayfair',
            'city': 'London',
            'address_length': (80, 120),
            'agency_length': (40, 80)
        },
        'stress_extreme_values': {
            'properties': 30,
            'mode': 'extreme_numbers',
            'vertical': 'real_estate',
            'area': 'Mayfair',
            'city': 'London',
            'price_range': (50000, 50000000),
            'days_range': (0, 365)
        },
        'stress_dense_tables': {
            'properties': 100,
            'mode': 'high_volume',
            'vertical': 'real_estate',
            'area': 'Mayfair',
            'city': 'London',
            'submarkets': 15
        },
        'stress_sparse': {
            'properties': 2,
            'mode': 'minimal',
            'vertical': 'real_estate',
            'area': 'Mayfair',
            'city': 'London',
            'submarkets': 1
        },
        'stress_nulls': {
            'properties': 20,
            'mode': 'missing_data',
            'vertical': 'real_estate',
            'area': 'Mayfair',
            'city': 'London',
            'null_rate': 0.3
        },
        'stress_zero_properties': {
            'properties': 0,
            'mode': 'empty',
            'vertical': 'real_estate',
            'area': 'Mayfair',
            'city': 'London'
        },
        'stress_single_property': {
            'properties': 1,
            'mode': 'singleton',
            'vertical': 'real_estate',
            'area': 'Mayfair',
            'city': 'London'
        }
    }
    
    def __init__(self):
        """Initialize factory with random seed for reproducibility"""
        random.seed(42)  # Reproducible results
    
    def generate_scenario(self, scenario_name: str) -> Dict[str, Any]:
        """
        Generate complete dataset for named scenario.
        
        Args:
            scenario_name: One of the SCENARIOS keys
            
        Returns:
            Complete Voxmill data structure ready for PDF generation
        """
        if scenario_name not in self.SCENARIOS:
            raise ValueError(f"Unknown scenario: {scenario_name}. Available: {list(self.SCENARIOS.keys())}")
        
        config = self.SCENARIOS[scenario_name]
        
        print(f"\n🎲 GENERATING SCENARIO: {scenario_name}")
        print(f"   Properties: {config['properties']}")
        print(f"   Mode: {config['mode']}")
        
        # Generate properties based on mode
        properties = self._generate_properties(config)
        
        # Calculate metrics
        metrics = self._calculate_metrics(properties, config)
        
        # Generate intelligence
        intelligence = self._generate_intelligence(metrics, config)
        
        # Build complete data structure
        data = {
            'metadata': {
                'vertical': {
                    'type': config.get('vertical', 'real_estate'),
                    'vertical_name': 'Real Estate',
                    'unit_metric': 'sqft',
                    'inventory_label': 'Active Listings',
                    'value_metric_label': 'Price',
                    'velocity_metric_label': 'Absorption Rate',
                    'market_signal_label': 'value signals',
                    'acquisition_label': 'Acquisition',
                    'forward_indicator_label': 'Price Momentum',
                    'currency_symbol': '£'
                },
                'area': config.get('area', 'Mayfair'),
                'city': config.get('city', 'London'),
                'timestamp': datetime.now().isoformat(),
                'data_source': f'Stress Test Scenario: {scenario_name}',
                'property_count': len(properties)
            },
            'kpis': metrics,
            'metrics': metrics,  # Duplicate for compatibility
            'properties': properties,
            'top_opportunities': properties[:15] if len(properties) > 15 else properties,
            'intelligence': intelligence
        }
        
        print(f"   ✅ Generated {len(properties)} properties")
        print(f"   ✅ Avg Price: £{metrics['avg_price']:,.0f}")
        print(f"   ✅ Avg Days: {metrics['days_on_market']}")
        
        return data
    
    def _generate_properties(self, config: Dict) -> List[Dict]:
        """Generate property list based on scenario config"""
        
        count = config['properties']
        mode = config['mode']
        
        if count == 0:
            return []
        
        properties = []
        
        # Determine submarkets
        num_submarkets = config.get('submarkets', 3)
        submarkets = self.SUBMARKETS_MAYFAIR[:num_submarkets]
        
        for i in range(count):
            prop = self._generate_single_property(i, mode, config, submarkets)
            properties.append(prop)
        
        return properties
    
    def _generate_single_property(
        self, 
        idx: int, 
        mode: str, 
        config: Dict,
        submarkets: List[str]
    ) -> Dict:
        """Generate a single property based on mode"""
        
        # Base property structure
        prop = {
            'listing_id': f'STRESS_{idx+1:03d}',
            'source': 'Stress Test Generator'
        }
        
        # MODE: Normal
        if mode == 'normal':
            prop.update({
                'address': f"{random.randint(1, 99)} {random.choice(self.STREET_NAMES_SHORT)}, {config['area']}, {config['city']}",
                'area': config['area'],
                'city': config['city'],
                'submarket': random.choice(submarkets),
                'district': random.choice(submarkets),
                'price': random.randint(1500000, 8000000),
                'beds': random.randint(2, 6),
                'baths': random.randint(2, 4),
                'sqft': random.randint(1500, 5000),
                'property_type': random.choice(self.PROPERTY_TYPES),
                'agent': random.choice(self.AGENCIES_SHORT),
                'url': 'https://example.com',
                'description': f"Stunning {random.choice(self.PROPERTY_TYPES).lower()} in prime {config['area']} location.",
                'image_url': '',
                'listed_date': (datetime.now() - timedelta(days=random.randint(14, 90))).strftime('%Y-%m-%d'),
                'days_listed': random.randint(14, 90),
                'days_on_market': random.randint(14, 90)
            })
            prop['price_per_sqft'] = round(prop['price'] / prop['sqft'], 2)
        
        # MODE: Extreme Text (long labels)
        elif mode == 'extreme_text':
            long_street = random.choice(self.STREET_NAMES_LONG)
            long_agency = random.choice(self.AGENCIES_LONG)
            
            prop.update({
                'address': f"{random.randint(1, 999)} {long_street}, {config['area']} Premium District, {config['city']} W1K",
                'area': config['area'],
                'city': config['city'],
                'submarket': random.choice(submarkets),
                'district': random.choice(submarkets),
                'price': random.randint(2000000, 10000000),
                'beds': random.randint(3, 7),
                'baths': random.randint(2, 5),
                'sqft': random.randint(2000, 6000),
                'property_type': random.choice(self.PROPERTY_TYPES),
                'agent': long_agency,
                'url': 'https://example.com',
                'description': f"Exceptional {random.choice(self.PROPERTY_TYPES).lower()} situated in the prestigious {long_street} conservation area.",
                'image_url': '',
                'listed_date': (datetime.now() - timedelta(days=random.randint(20, 120))).strftime('%Y-%m-%d'),
                'days_listed': random.randint(20, 120),
                'days_on_market': random.randint(20, 120)
            })
            prop['price_per_sqft'] = round(prop['price'] / prop['sqft'], 2)
        
        # MODE: Extreme Numbers
        elif mode == 'extreme_numbers':
            price_range = config.get('price_range', (50000, 50000000))
            days_range = config.get('days_range', (0, 365))
            
            price = random.randint(price_range[0], price_range[1])
            days = random.randint(days_range[0], days_range[1])
            sqft = random.randint(500, 10000)
            
            prop.update({
                'address': f"{random.randint(1, 99)} {random.choice(self.STREET_NAMES_SHORT)}, {config['area']}",
                'area': config['area'],
                'city': config['city'],
                'submarket': random.choice(submarkets),
                'district': random.choice(submarkets),
                'price': price,
                'beds': random.randint(1, 8),
                'baths': random.randint(1, 6),
                'sqft': sqft,
                'property_type': random.choice(self.PROPERTY_TYPES),
                'agent': random.choice(self.AGENCIES_SHORT),
                'url': 'https://example.com',
                'description': f"Property with extreme valuation metrics.",
                'image_url': '',
                'listed_date': (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d'),
                'days_listed': days,
                'days_on_market': days
            })
            prop['price_per_sqft'] = round(prop['price'] / prop['sqft'], 2)
        
        # MODE: High Volume / Minimal / Missing Data
        else:
            prop.update({
                'address': f"{random.randint(1, 99)} {random.choice(self.STREET_NAMES_SHORT)}, {config['area']}",
                'area': config['area'],
                'city': config['city'],
                'submarket': random.choice(submarkets),
                'district': random.choice(submarkets),
                'price': random.randint(1000000, 12000000),
                'beds': random.randint(2, 5),
                'baths': random.randint(2, 4),
                'sqft': random.randint(1200, 4500),
                'property_type': random.choice(self.PROPERTY_TYPES),
                'agent': random.choice(self.AGENCIES_SHORT) if random.random() > config.get('null_rate', 0) else 'Private',
                'url': 'https://example.com',
                'description': f"Standard property listing.",
                'image_url': '',
                'listed_date': (datetime.now() - timedelta(days=random.randint(10, 100))).strftime('%Y-%m-%d'),
                'days_listed': random.randint(10, 100),
                'days_on_market': random.randint(10, 100)
            })
            prop['price_per_sqft'] = round(prop['price'] / prop['sqft'], 2)
            
            # Apply nulls if missing_data mode
            if mode == 'missing_data':
                null_rate = config.get('null_rate', 0.3)
                if random.random() < null_rate:
                    prop['price_per_sqft'] = 0
                if random.random() < null_rate:
                    prop['beds'] = 0
                if random.random() < null_rate:
                    prop['agent'] = ''
        
        return prop
    
    def _calculate_metrics(self, properties: List[Dict], config: Dict) -> Dict:
        """Calculate KPIs from property list"""
        
        if not properties:
            return {
                'total_properties': 0,
                'property_change': 0,
                'avg_price': 0,
                'price_change': 0,
                'avg_price_per_sqft': 0,
                'sqft_change': 0,
                'days_on_market': 42,
                'velocity_change': 0
            }
        
        prices = [p['price'] for p in properties if p.get('price', 0) > 0]
        prices_per_sqft = [p['price_per_sqft'] for p in properties if p.get('price_per_sqft', 0) > 0]
        days_list = [p['days_on_market'] for p in properties if p.get('days_on_market', 0) > 0]
        
        return {
            'total_properties': len(properties),
            'property_change': random.randint(-10, 10),
            'avg_price': int(sum(prices) / len(prices)) if prices else 0,
            'price_change': round(random.uniform(-5, 5), 1),
            'avg_price_per_sqft': int(sum(prices_per_sqft) / len(prices_per_sqft)) if prices_per_sqft else 0,
            'sqft_change': round(random.uniform(-3, 3), 1),
            'days_on_market': int(sum(days_list) / len(days_list)) if days_list else 42,
            'velocity_change': round(random.uniform(-8, 8), 1)
        }
    
    def _generate_intelligence(self, metrics: Dict, config: Dict) -> Dict:
        """Generate AI intelligence section"""
        
        area = config.get('area', 'Mayfair')
        total = metrics['total_properties']
        avg_price = metrics['avg_price']
        
        if total == 0:
            summary = f"Market analysis for {area} indicates limited active inventory. Strategic positioning recommended for emerging opportunities."
        else:
            summary = f"Market analysis of {total} properties in {area} reveals average pricing at £{avg_price:,.0f}. Current market conditions suggest balanced positioning with opportunities across multiple segments."
        
        return {
            'executive_summary': summary,
            'market_momentum': f"Market demonstrates {'strong' if metrics['price_change'] > 2 else 'stable'} momentum characteristics.",
            'price_positioning': f"Pricing {'premium' if avg_price > 5000000 else 'competitive'} relative to market benchmarks.",
            'velocity_signal': f"Transaction velocity {'improving' if metrics['velocity_change'] < 0 else 'stabilizing'}.",
            'strategic_insights': [
                f"Total market inventory: {total} active assets",
                f"Average price positioning: £{avg_price:,.0f}",
                f"Velocity dynamics: {metrics['days_on_market']} days average",
                "Strategic opportunities exist across multiple price bands"
            ]
        }


# ============================================================================
# CLI INTERFACE
# ============================================================================

def main():
    """CLI for generating stress test scenarios"""
    import sys
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Voxmill Stress Scenario Generator',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    
    parser.add_argument(
        'scenario',
        choices=list(VoxmillDataFactory.SCENARIOS.keys()),
        help='Scenario to generate'
    )
    
    parser.add_argument(
        '-o', '--output',
        default='/tmp/voxmill_stress_test.json',
        help='Output JSON file path'
    )
    
    args = parser.parse_args()
    
    factory = VoxmillDataFactory()
    data = factory.generate_scenario(args.scenario)
    
    with open(args.output, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"\n✅ Scenario data saved to: {args.output}")
    print(f"   Properties: {len(data['properties'])}")
    print(f"   Ready for PDF generation")


if __name__ == '__main__':
    main()
