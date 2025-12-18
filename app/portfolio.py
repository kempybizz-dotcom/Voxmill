"""
VOXMILL PORTFOLIO TRACKER
=========================
Track client property holdings and generate cross-property insights 
"""

import logging
from datetime import datetime, timezone
from pymongo import MongoClient
import os

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv('MONGODB_URI')
mongo_client = MongoClient(MONGODB_URI)
db = mongo_client['Voxmill']


def add_property_to_portfolio(whatsapp_number: str, property_data: dict) -> str:
    """Add property to client's portfolio"""
    
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
                'added_at': datetime.now(timezone.utc)
            }},
            '$set': {'updated_at': datetime.now(timezone.utc)}
        },
        upsert=True
    )
    
    return f"""PROPERTY ADDED TO PORTFOLIO

{property_data.get('address')}
Purchase: £{property_data.get('purchase_price'):,.0f}
Region: {property_data.get('region')}

Portfolio tracking active."""


def get_portfolio_summary(whatsapp_number: str) -> dict:
    """Get client's full portfolio with current valuations"""
    
    portfolio = db['client_portfolios'].find_one({'whatsapp_number': whatsapp_number})
    
    if not portfolio or not portfolio.get('properties'):
        return {'error': 'no_portfolio'}
    
    from app.dataset_loader import load_dataset
    
    enriched_properties = []
    total_purchase = 0
    total_current = 0
    
    for prop in portfolio['properties']:
        # Load market data for region
        dataset = load_dataset(area=prop['region'])
        
        # Estimate current value (simple method - match property type avg)
        current_estimate = dataset.get('metrics', {}).get('avg_price', prop['purchase_price'])
        
        gain_loss = current_estimate - prop['purchase_price']
        gain_loss_pct = (gain_loss / prop['purchase_price']) * 100
        
        enriched_properties.append({
            **prop,
            'current_estimate': current_estimate,
            'gain_loss': gain_loss,
            'gain_loss_pct': gain_loss_pct
        })
        
        total_purchase += prop['purchase_price']
        total_current += current_estimate
    
    return {
        'properties': enriched_properties,
        'total_purchase_value': total_purchase,
        'total_current_value': total_current,
        'total_gain_loss': total_current - total_purchase,
        'total_gain_loss_pct': ((total_current - total_purchase) / total_purchase) * 100,
        'property_count': len(enriched_properties)
    }
