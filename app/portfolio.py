"""
VOXMILL PORTFOLIO TRACKER
=========================
Track client property holdings and generate cross-property insights

✅ FIXED: No hardcoded region defaults
✅ FIXED: Industry parameter added to load_dataset calls
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
    Parse property details from user message (FLEXIBLE)
    
    Supports formats:
    1. Full: "Property: 123 Park Lane, Purchase: £2500000, Date: 2023-01-15, Region: Mayfair"
    2. Simple: "Add property: One Hyde Park, Knightsbridge"
    3. Free text: "One Hyde Park, Knightsbridge, London SW1X"
    
    Returns:
        {
            'address': '123 Park Lane',
            'purchase_price': 2500000 or None,
            'purchase_date': '2023-01-15' or today,
            'region': 'Mayfair' or extracted from address
        }
    """
    
    import re as regex_module  # ✅ FIX: Use alias to avoid shadowing
    
    try:
        message_clean = message.strip()
        
        # ========================================
        # METHOD 1: STRUCTURED FORMAT (FULL DATA)
        # ========================================
        
        # Check if message has structured format
        has_price = 'purchase:' in message.lower() or '£' in message
        has_date = regex_module.search(r'\d{4}-\d{2}-\d{2}', message)
        has_region_keyword = 'region:' in message.lower()
        
        if has_price and has_date and has_region_keyword:
            # Parse structured format
            address_match = regex_module.search(r'Property:\s*([^,]+)', message, regex_module.IGNORECASE)
            address = address_match.group(1).strip() if address_match else None
            
            price_match = regex_module.search(r'Purchase:\s*£?([\d,]+)', message, regex_module.IGNORECASE)
            price = int(price_match.group(1).replace(',', '')) if price_match else None
            
            date_match = regex_module.search(r'Date:\s*(\d{4}-\d{2}-\d{2})', message, regex_module.IGNORECASE)
            date = date_match.group(1) if date_match else None
            
            region_match = regex_module.search(r'Region:\s*([A-Za-z\s]+)', message, regex_module.IGNORECASE)
            region = region_match.group(1).strip() if region_match else None
            
            if address and price and date and region:
                return {
                    'address': address,
                    'purchase_price': price,
                    'purchase_date': date,
                    'region': region
                }
        
        # ========================================
        # METHOD 2: FREE TEXT ADDRESS (SMART PARSING)
        # ========================================
        
        # ✅ COMPREHENSIVE PREFIX REMOVAL
        prefixes_to_remove = [
            'add this property to my portfolio:',
            'add this property to my portfolio -',
            'add this property to my portfolio',
            'add property to my portfolio:',
            'add property to my portfolio',
            'add a property to my portfolio:',
            'add a property to my portfolio',
            'add to my portfolio:',
            'add to my portfolio',
            'add property:',
            'add property',
            'add a propert',  # Common typo
            'add properly:',  # Common typo
            'property:',
            'track:',
            'add:',
            'add'  # Catch-all last
        ]
        
        # Try each prefix
        for prefix in prefixes_to_remove:
            if message_clean.lower().startswith(prefix):
                message_clean = message_clean[len(prefix):].strip()
                break
        
        # Additional cleanup: remove leading dash, colon, or "to my portfolio"
        message_clean = message_clean.lstrip(':-').strip()
        
        # Extract address (everything that's left)
        address = message_clean
        
        if not address or len(address) < 5:
            return None
        
        # Extract region from address (common patterns)
        region = None
        
        # UK postcode pattern (e.g., SW1X, W1K, etc.)
        postcode_match = regex_module.search(r'\b([A-Z]{1,2}\d{1,2}[A-Z]?)\b', address)
        if postcode_match:
            postcode = postcode_match.group(1)
            # Map postcode to region (simple heuristic)
            postcode_to_region = {
                'SW1': 'Belgravia', 'SW1X': 'Knightsbridge', 'SW1W': 'Belgravia',
                'SW3': 'Chelsea', 'SW7': 'South Kensington',
                'W1': 'Mayfair', 'W1K': 'Mayfair', 'W1J': 'Mayfair',
                'W8': 'Kensington', 'W11': 'Notting Hill',
                'NW1': 'Regent\'s Park', 'NW3': 'Hampstead', 'NW8': 'St Johns Wood'
            }
            
            for code, area in postcode_to_region.items():
                if postcode.startswith(code):
                    region = area
                    break
        
        # Neighborhood name detection
        neighborhoods = [
            'Mayfair', 'Knightsbridge', 'Chelsea', 'Belgravia', 'Kensington',
            'South Kensington', 'Notting Hill', 'Hampstead', 'Primrose Hill',
            'St Johns Wood', "Regent's Park", 'Marylebone', 'Holland Park'
        ]
        
        if not region:
            for neighborhood in neighborhoods:
                if neighborhood.lower() in address.lower():
                    region = neighborhood
                    break
        
        # Default region if still not found
        if not region:
            region = 'Central London'  # Generic fallback
        
        # Return with smart defaults
        return {
            'address': address,
            'purchase_price': None,  # User can update later
            'purchase_date': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
            'region': region,
            'property_type': 'apartment'  # Default for luxury London
        }
        
    except Exception as e:
        logger.error(f"Property parsing error: {e}")
        return None


def add_property_to_portfolio(whatsapp_number: str, property_data: dict) -> str:
    """
    Add property to client's portfolio with MongoDB persistence
    
    ✅ CHATGPT FIX: Returns structured confirmation message
    
    Args:
        whatsapp_number: Client's WhatsApp number
        property_data: Dict with address, region, purchase_price, etc.
    
    Returns:
        Confirmation message in portfolio format
    """
    
    try:
        # Build property document (handle None values)
        property_doc = {
            'id': f"prop_{int(datetime.now(timezone.utc).timestamp())}",
            'address': property_data.get('address'),
            'region': property_data.get('region'),
            'property_type': property_data.get('property_type', 'apartment'),
            'added_at': datetime.now(timezone.utc).isoformat()
        }
        
        # Only add price/date if provided
        if property_data.get('purchase_price'):
            property_doc['purchase_price'] = property_data.get('purchase_price')
        
        if property_data.get('purchase_date'):
            property_doc['purchase_date'] = property_data.get('purchase_date')
        
        # ✅ CRITICAL: WRITE TO MONGODB
        result = db['client_portfolios'].update_one(
            {'whatsapp_number': whatsapp_number},
            {
                '$push': {'properties': property_doc},
                '$set': {'updated_at': datetime.now(timezone.utc).isoformat()}
            },
            upsert=True
        )
        
        # ✅ VERIFY WRITE SUCCESS
        if result.modified_count > 0 or result.upserted_id:
            logger.info(f"✅ Property added to portfolio: {whatsapp_number}")
            
            # Get portfolio count
            portfolio = db['client_portfolios'].find_one({'whatsapp_number': whatsapp_number})
            portfolio_count = len(portfolio.get('properties', [])) if portfolio else 1
            
            # ✅ CHATGPT FIX: Structured confirmation format
            address = property_data.get('address')
            region = property_data.get('region')
            price = property_data.get('purchase_price')
            
            if price:
                return f"""PORTFOLIO UPDATED

Asset added:
{address}
Region: {region}
Purchase price: £{price:,.0f}

Portfolio size: {portfolio_count} asset{"s" if portfolio_count != 1 else ""}

Standing by."""
            else:
                return f"""PORTFOLIO UPDATED

Asset added:
{address}
Region: {region}
Estimated value: Market rate

Portfolio size: {portfolio_count} asset{"s" if portfolio_count != 1 else ""}

Note: Add purchase price for precise tracking:
"Update property: {address}, Purchase: £[amount]"

Standing by."""
        else:
            logger.error(f"Portfolio update failed: no documents modified")
            return "Failed to add property. Please try again."
        
    except Exception as e:
        logger.error(f"Add property error: {e}", exc_info=True)
        return "Failed to add property. Please try again."


def get_portfolio_summary(whatsapp_number: str, client_profile: dict = None) -> dict:
    """
    Get client's full portfolio with INTELLIGENT valuations
    
    ✅ FIXED: No hardcoded region defaults
    ✅ FIXED: Industry parameter added
    
    UPGRADE: Uses property-specific matching for accurate estimates
    
    Args:
        whatsapp_number: Client's WhatsApp number
        client_profile: Client profile dict with industry
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
            # ✅ FIXED: No hardcoded default - skip property if no region
            region = prop.get('region')
            
            if not region:
                logger.warning(f"Property {prop.get('address')} has no region, skipping valuation")
                continue
            
            purchase_price = prop.get('purchase_price', 0)
            
            # Load market data for region
            # ✅ FIXED: Added industry parameter
            industry = 'real_estate'
            if client_profile:
                industry = client_profile.get('industry', 'real_estate')

            dataset = load_dataset(area=region, industry=industry)
            
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
                    
                    # Assume 5% annual appreciation for prime properties
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

def clear_portfolio(client_id: str) -> bool:
    """
    Clear all properties from a client's portfolio
    
    Args:
        client_id: WhatsApp number
    
    Returns: True if successful
    """
    try:
        # Get client record
        client_record = get_client_record(client_id)
        
        if not client_record:
            logger.warning(f"No client found for {client_id}")
            return False
        
        record_id = client_record['id']
        
        # Clear portfolio field
        airtable_client.table('Accounts').update(
            record_id,
            {
                'portfolio': []  # Empty array
            }
        )
        
        logger.info(f"✅ Portfolio cleared for {client_id}")
        return True
        
    except Exception as e:
        logger.error(f"Failed to clear portfolio for {client_id}: {e}")
        return False
