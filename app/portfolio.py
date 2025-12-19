"""
VOXMILL PORTFOLIO TRACKER
=========================
Track client property holdings and generate cross-property insights 
"""

import logging
import re
from datetime import datetime, timezone
from typing import Dict, Optional
from pymongo import MongoClient
import os

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv('MONGODB_URI')
mongo_client = MongoClient(MONGODB_URI)
db = mongo_client['Voxmill']


def parse_property_from_message(message: str) -> Optional[Dict]:
    """
    Parse property details from user message
    
    Expected format:
    "Property: 123 Park Lane, Purchase: £2500000, Date: 2023-01-15, Region: Mayfair"
    
    Returns:
        {
            'address': '123 Park Lane',
            'purchase_price': 2500000,
            'purchase_date': '2023-01-15',
            'region': 'Mayfair'
        }
    """
    
    try:
        # Extract address
        address_match = re.search(r'Property:\s*([^,]+)', message, re.IGNORECASE)
        address = address_match.group(1).strip() if address_match else None
        
        # Extract price
        price_match = re.search(r'Purchase:\s*£?([\d,]+)', message, re.IGNORECASE)
        price = int(price_match.group(1).replace(',', '')) if price_match else None
        
        # Extract date
        date_match = re.search(r'Date:\s*(\d{4}-\d{2}-\d{2})', message, re.IGNORECASE)
        date = date_match.group(1) if date_match else None
        
        # Extract region
        region_match = re.search(r'Region:\s*([A-Za-z\s]+)', message, re.IGNORECASE)
        region = region_match.group(1).strip() if region_match else None
        
        # Validate all fields present
        if not all([address, price, date, region]):
            return None
        
        return {
            'address': address,
            'purchase_price': price,
            'purchase_date': date,
            'region': region
        }
        
    except Exception as e:
        logger.error(f"Property parsing error: {e}")
        return None


def add_property_to_portfolio(whatsapp_number: str, property_data: dict) -> str:
    """Add property to client's portfolio"""
    
    try:
        db['client_portfolios'].update_one(
            {'whatsapp_number': whatsapp_number},
            {
                '$push': {'properties': {
                    'id': f"prop_{int(datetime.now(timezone.utc).timestamp())}",
                    'address': property_data.get('address'),
                    'purchase_price': property_data.get('purchase_price'),
                    'purchase_date': property_data.get('purchase_date'),
                    'region': property_data.get('region'),
                    'property_type': property_data.get('property_type'),
                    'added_at': datetime.now(timezone.utc).isoformat()
                }},
                '$set': {'updated_at': datetime.now(timezone.utc).isoformat()}
            },
            upsert=True
        )
        
        logger.info(f"✅ Property added to portfolio: {whatsapp_number}")
        
        return f"""PROPERTY ADDED TO PORTFOLIO

{property_data.get('address')}
Purchase: £{property_data.get('purchase_price'):,.0f}
Region: {property_data.get('region')}

Portfolio tracking active."""
        
    except Exception as e:
        logger.error(f"Add property error: {e}", exc_info=True)
        return "Failed to add property. Please try again."


def get_portfolio_summary(whatsapp_number: str) -> dict:
    """
    Get client's full portfolio with INTELLIGENT valuations
    
    UPGRADE: Uses property-specific matching for accurate estimates
    """
    
    try:
        portfolio = db['client_portfolios'].find_one({'whatsapp_number': whatsapp_number})
        
        if not portfolio or not portfolio.get('properties'):
            return {'error': 'no_portfolio'}
        
        from app.dataset_loader import load_dataset
        import statistics
        
        enriched_properties = []
        total_purchase = 0
        total_current = 0
        
        for prop in portfolio['properties']:
            region = prop.get('region', 'Mayfair')
            purchase_price = prop.get('purchase_price', 0)
            
            # Load market data for region
            dataset = load_dataset(area=region)
            
            # INTELLIGENT VALUATION
            # Match similar properties in market data
            market_properties = dataset.get('properties', [])
            
            similar_properties = [
                p for p in market_properties
                if p.get('property_type') == prop.get('property_type')
            ]
            
            if similar_properties:
                # Use similar property average
                similar_prices = [p['price'] for p in similar_properties if p.get('price')]
                current_estimate = int(statistics.median(similar_prices)) if similar_prices else purchase_price
            else:
                # Fallback to regional average with appreciation
                avg_price = dataset.get('metrics', {}).get('avg_price', purchase_price)
                
                # Calculate days since purchase
                try:
                    purchase_date = datetime.fromisoformat(prop.get('purchase_date', '2023-01-01'))
                    days_held = (datetime.now(timezone.utc) - purchase_date).days
                    
                    # Assume 5% annual appreciation for prime London
                    annual_appreciation = 0.05
                    years_held = days_held / 365
                    appreciation_multiplier = (1 + annual_appreciation) ** years_held
                    
                    current_estimate = int(purchase_price * appreciation_multiplier)
                except:
                    current_estimate = avg_price
            
            gain_loss = current_estimate - purchase_price
            gain_loss_pct = (gain_loss / purchase_price) * 100 if purchase_price > 0 else 0
            
            enriched_properties.append({
                **prop,
                'current_estimate': current_estimate,
                'gain_loss': gain_loss,
                'gain_loss_pct': round(gain_loss_pct, 1)
            })
            
            total_purchase += purchase_price
            total_current += current_estimate
        
        total_gain_loss = total_current - total_purchase
        total_gain_loss_pct = (total_gain_loss / total_purchase) * 100 if total_purchase > 0 else 0
        
        return {
            'properties': enriched_properties,
            'total_purchase_value': total_purchase,
            'total_current_value': total_current,
            'total_gain_loss': total_gain_loss,
            'total_gain_loss_pct': round(total_gain_loss_pct, 1),
            'property_count': len(enriched_properties)
        }
        
    except Exception as e:
        logger.error(f"Portfolio summary error: {e}", exc_info=True)
        return {'error': 'calculation_failed'}
