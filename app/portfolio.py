"""
VOXMILL PORTFOLIO TRACKER - INSTITUTIONAL GRADE
===============================================
Track client property holdings with deterministic command grammar

ARCHITECTURE:
- Command Grammar Parser (deterministic, no keywords)
- FSM Integration (destructive ops require confirmation)
- MongoDB Persistence (single source of truth)
- NO HARDCODED REGIONS (works for any industry/geography)

CHATGPT FSM COMPLIANCE:
- Destructive commands (reset/remove) create pending actions
- Non-destructive commands (add/view) execute immediately
- No free-text parsing into state mutations
"""

import logging
import re
from datetime import datetime, timezone
from typing import Dict, Optional
from dataclasses import dataclass
from enum import Enum
from pymongo import MongoClient
import os

logger = logging.getLogger(__name__)

MONGODB_URI = os.getenv('MONGODB_URI')
mongo_client = MongoClient(MONGODB_URI)
db = mongo_client['Voxmill']


# ============================================================================
# COMMAND GRAMMAR (INSTITUTIONAL PROTOCOL)
# ============================================================================

class CommandType(Enum):
    """Portfolio command types"""
    ADD = "add"
    REMOVE = "remove"
    RESET = "reset"
    VIEW = "view"


@dataclass
class PortfolioCommand:
    """Parsed portfolio command"""
    type: CommandType
    args: tuple = ()
    
    @property
    def is_destructive(self) -> bool:
        """Check if command is destructive (requires confirmation)"""
        return self.type in [CommandType.RESET, CommandType.REMOVE]


def parse_portfolio_command(message: str) -> Optional[PortfolioCommand]:
    """
    Parse portfolio command using deterministic grammar
    
    INSTITUTIONAL PROTOCOL:
    - Explicit syntax only (no free-text parsing)
    - Returns None if no match (not a portfolio command)
    - No keyword soup - exact grammar matching
    
    Grammar:
        add property: <address>[, region: <market>]
        remove property: <address>
        reset portfolio
        show portfolio | view portfolio
    
    Args:
        message: User message text
    
    Returns: PortfolioCommand or None
    """
    
    message_clean = message.strip()
    
    # Grammar patterns (exact matching - case insensitive)
    patterns = {
        CommandType.ADD: r'^add property:\s*(.+)$',
        CommandType.REMOVE: r'^remove property:\s*(.+)$',
        CommandType.RESET: r'^reset portfolio$',
        CommandType.VIEW: r'^(show|view) portfolio$'
    }
    
    for cmd_type, pattern in patterns.items():
        match = re.match(pattern, message_clean, re.IGNORECASE)
        
        if match:
            args = match.groups() if match.groups() else ()
            logger.info(f"✅ Command matched: {cmd_type.value} with args: {args}")
            return PortfolioCommand(type=cmd_type, args=args)
    
    return None


async def execute_portfolio_command(client_id: str, command: PortfolioCommand, 
                                    client_profile: Dict) -> str:
    """
    Execute portfolio command (deterministic routing)
    
    CRITICAL FSM COMPLIANCE:
    - Destructive commands (reset/remove) → Create pending action, NOT execute
    - Non-destructive commands (add/view) → Execute immediately
    
    Args:
        client_id: WhatsApp number
        command: Parsed command
        client_profile: Client profile dict
    
    Returns: Response message
    """
    
    from app.pending_actions import action_manager, ActionType
    
    # ========================================
    # VIEW (Non-destructive - execute immediately)
    # ========================================
    
    if command.type == CommandType.VIEW:
        portfolio = get_portfolio_summary(client_id, client_profile)
        
        if portfolio.get('error'):
            return """Your portfolio is currently empty.

You can add properties by sending:
add property: [address], region: [market]"""
        
        total_properties = len(portfolio.get('properties', []))
        
        if total_properties == 0:
            return """Your portfolio is currently empty.

You can add properties by sending:
add property: [address], region: [market]"""
        
        total_value = portfolio.get('total_current_value', 0)
        total_gain_loss = portfolio.get('total_gain_loss_pct', 0)
        
        # Build property list (max 5)
        prop_list = []
        for prop in portfolio.get('properties', [])[:5]:
            address = prop.get('address', 'Unknown')
            current_estimate = prop.get('current_estimate', 0)
            gain_loss_pct = prop.get('gain_loss_pct', 0)
            
            prop_list.append(
                f"• {address}: £{current_estimate:,.0f} ({gain_loss_pct:+.1f}%)"
            )
        
        return f"""PORTFOLIO SUMMARY

{chr(10).join(prop_list)}

Total: {total_properties} properties
Value: £{total_value:,.0f} ({total_gain_loss:+.1f}%)"""
    
    # ========================================
    # ADD (Non-destructive - execute immediately)
    # ========================================
    
    elif command.type == CommandType.ADD:
        address_raw = command.args[0] if command.args else None
        
        if not address_raw or len(address_raw) < 3:
            active_market = client_profile.get('active_market', 'your market')
            
            return f"""PROPERTY FORMAT

Format:
add property: [address], region: [market]

Your active market: {active_market}

Example:
add property: 123 Main Street, region: {active_market}

Or use your active market as default:
add property: 123 Main Street

Standing by."""
        
        # Parse property (passes client_profile for active_market default)
        property_data = parse_property_from_message(
            address_raw, 
            client_profile=client_profile
        )
        
        if not property_data:
            active_market = client_profile.get('active_market', 'your market')
            
            return f"""PROPERTY NOT RECOGNIZED

Please provide a valid address.

Your active market: {active_market}

Format:
add property: [address], region: [market]

Example:
add property: 123 Main Street, region: {active_market}

Standing by."""
        
        # Execute add immediately
        response = add_property_to_portfolio(client_id, property_data)
        return response
    
    # ========================================
    # RESET (Destructive - create pending action)
    # ========================================
    
    elif command.type == CommandType.RESET:
        # Get current portfolio count
        portfolio = get_portfolio_summary(client_id, client_profile)
        property_count = len(portfolio.get('properties', [])) if not portfolio.get('error') else 0
        
        try:
            # Create pending action (DON'T execute)
            pending = action_manager.create_action(
                client_id,
                ActionType.RESET_PORTFOLIO,
                data={'property_count': property_count}
            )
            
            return f"""PORTFOLIO RESET REQUESTED

No action taken.

Current portfolio: {property_count} asset{"s" if property_count != 1 else ""}

Reply: CONFIRM RESET {pending.action_id} within 5 minutes."""
        
        except ValueError as e:
            # Already has pending action
            return str(e)
    
    # ========================================
    # REMOVE (Destructive - create pending action)
    # ========================================
    
    elif command.type == CommandType.REMOVE:
        address = command.args[0] if command.args else None
        
        if not address or len(address) < 3:
            return """REMOVAL FORMAT

Specify property:
remove property: [address]

Example:
remove property: 123 Main Street

Standing by."""
        
        try:
            # Create pending action (DON'T execute)
            pending = action_manager.create_action(
                client_id,
                ActionType.REMOVE_PROPERTY,
                data={'address': address}
            )
            
            return f"""PROPERTY REMOVAL REQUESTED

No action taken.

Target: {address}

Reply: CONFIRM REMOVE {pending.action_id} within 5 minutes."""
        
        except ValueError as e:
            # Already has pending action
            return str(e)


# ============================================================================
# PROPERTY PARSING (NO HARDCODED REGIONS)
# ============================================================================

def parse_property_from_message(address_raw: str, client_profile: Dict = None) -> Optional[Dict]:
    """
    Parse property details from user input (NO HARDCODED REGIONS)
    
    ✅ FIXED: No postcode mappings, no neighborhood lists
    ✅ Uses client's active_market as default
    ✅ Supports explicit region syntax
    
    Supported formats:
    1. Explicit region: "123 Park Lane, region: Mayfair"
    2. Use active market: "123 Park Lane" (uses client_profile.active_market)
    3. Structured: "Property: 123 Park Lane, Purchase: £2500000, Date: 2023-01-15, Region: Mayfair"
    
    Args:
        address_raw: Raw address string (already has "add property:" removed)
        client_profile: Client profile with active_market
    
    Returns:
        {
            'address': '123 Park Lane',
            'purchase_price': 2500000 or None,
            'purchase_date': '2023-01-15' or today,
            'region': 'Mayfair' (from explicit syntax or active_market)
        }
    """
    
    try:
        address_clean = address_raw.strip()
        
        # ========================================
        # METHOD 1: STRUCTURED FORMAT (FULL DATA)
        # ========================================
        
        # Check if message has structured format
        has_price = 'purchase:' in address_clean.lower()
        has_date = re.search(r'\d{4}-\d{2}-\d{2}', address_clean)
        has_region_keyword = 'region:' in address_clean.lower()
        
        if has_price and has_date and has_region_keyword:
            # Parse structured format
            property_match = re.search(r'Property:\s*([^,]+)', address_clean, re.IGNORECASE)
            property_addr = property_match.group(1).strip() if property_match else None
            
            price_match = re.search(r'Purchase:\s*£?([\d,]+)', address_clean, re.IGNORECASE)
            price = int(price_match.group(1).replace(',', '')) if price_match else None
            
            date_match = re.search(r'Date:\s*(\d{4}-\d{2}-\d{2})', address_clean, re.IGNORECASE)
            date = date_match.group(1) if date_match else None
            
            region_match = re.search(r'Region:\s*([A-Za-z\s]+)', address_clean, re.IGNORECASE)
            region = region_match.group(1).strip() if region_match else None
            
            if property_addr and price and date and region:
                return {
                    'address': property_addr,
                    'purchase_price': price,
                    'purchase_date': date,
                    'region': region,
                    'property_type': 'asset'
                }
        
        # ========================================
        # METHOD 2: EXPLICIT REGION SYNTAX
        # ========================================
        
        # Pattern: "123 Park Lane, region: Mayfair"
        region_match = re.search(r',\s*region:\s*([A-Za-z\s]+)$', address_clean, re.IGNORECASE)
        
        if region_match:
            region = region_match.group(1).strip()
            address = address_clean[:region_match.start()].strip()
            
            if address and len(address) >= 5:
                return {
                    'address': address,
                    'region': region,
                    'purchase_price': None,
                    'purchase_date': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
                    'property_type': 'asset'
                }
        
        # ========================================
        # METHOD 3: USE CLIENT'S ACTIVE MARKET
        # ========================================
        
        # No explicit region - use client's active_market as default
        address = address_clean
        
        if not address or len(address) < 5:
            return None
        
        # Get region from client profile
        if client_profile:
            region = client_profile.get('active_market')
        else:
            region = None
        
        if not region:
            # Cannot proceed without region
            logger.warning(f"Property parsing failed: no region available for '{address}'")
            return None
        
        return {
            'address': address,
            'region': region,
            'purchase_price': None,
            'purchase_date': datetime.now(timezone.utc).strftime('%Y-%m-%d'),
            'property_type': 'asset'
        }
        
    except Exception as e:
        logger.error(f"Property parsing error: {e}")
        return None


# ============================================================================
# PORTFOLIO OPERATIONS (MONGODB)
# ============================================================================

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
            'property_type': property_data.get('property_type', 'asset'),
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
    
    ✅ CHATGPT FIX: Direct MongoDB write, no undefined functions
    
    Args:
        client_id: WhatsApp number
    
    Returns: True if successful
    """
    try:
        # Direct MongoDB update (no get_client_record needed)
        result = db['client_portfolios'].update_one(
            {'whatsapp_number': client_id},
            {
                '$set': {
                    'properties': [],
                    'updated_at': datetime.now(timezone.utc).isoformat()
                }
            }
        )
        
        if result.modified_count > 0:
            logger.info(f"✅ Portfolio cleared for {client_id}")
            return True
        else:
            # No existing portfolio to clear
            logger.info(f"ℹ️ No portfolio found for {client_id}, nothing to clear")
            return True
        
    except Exception as e:
        logger.error(f"Failed to clear portfolio for {client_id}: {e}")
        return False


def remove_property_from_portfolio(client_id: str, property_address: str) -> Dict:
    """
    Remove specific property from portfolio
    
    Args:
        client_id: WhatsApp number
        property_address: Exact address to remove
    
    Returns: {success: bool, message: str}
    """
    try:
        # Get portfolio
        portfolio = db['client_portfolios'].find_one({'whatsapp_number': client_id})
        
        if not portfolio or not portfolio.get('properties'):
            return {'success': False, 'message': 'Portfolio is empty.'}
        
        properties = portfolio['properties']
        
        # Find matching property (exact or fuzzy)
        matches = [p for p in properties if property_address.lower() in p['address'].lower()]
        
        if not matches:
            return {
                'success': False,
                'message': f"Property not found: {property_address}\n\nCurrent portfolio:\n" + 
                          "\n".join([f"• {p['address']}" for p in properties[:5]])
            }
        
        if len(matches) > 1:
            return {
                'success': False,
                'message': f"Multiple matches found:\n" +
                          "\n".join([f"• {p['address']}" for p in matches]) +
                          "\n\nPlease be more specific."
            }
        
        # Remove property
        property_to_remove = matches[0]
        
        result = db['client_portfolios'].update_one(
            {'whatsapp_number': client_id},
            {
                '$pull': {'properties': {'address': property_to_remove['address']}},
                '$set': {'updated_at': datetime.now(timezone.utc).isoformat()}
            }
        )
        
        if result.modified_count > 0:
            return {
                'success': True,
                'message': f"PORTFOLIO UPDATED\n\nRemoved:\n{property_to_remove['address']}\n\nStanding by."
            }
        else:
            return {'success': False, 'message': 'Removal failed. Please try again.'}
        
    except Exception as e:
        logger.error(f"Remove property error: {e}", exc_info=True)
        return {'success': False, 'message': 'Removal failed. Please try again.'}
