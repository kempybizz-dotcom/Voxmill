"""
VOXMILL COMPETITOR IDENTIFIER
==============================
Identifies and analyzes competitors specific to the client's agency

✅ Agency-aware competitor detection
✅ Market share calculation
✅ Positioning analysis
✅ Threat/opportunity assessment
"""

import logging
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class CompetitorIdentifier:
    """Identifies competitors specific to client's agency"""
    
    @staticmethod
    def identify_competitors(
        agency_name: str,
        dataset: Dict,
        max_competitors: int = 5
    ) -> Dict:
        """
        Identify competitors for a specific agency from dataset
        
        Args:
            agency_name: Client's agency name (e.g., "Wetherell")
            dataset: Loaded dataset with properties
            max_competitors: Maximum number of competitors to return
        
        Returns:
            Competitor intelligence dict
        """
        
        if not agency_name:
            logger.warning("No agency name provided for competitor identification")
            return {
                'error': 'no_agency',
                'message': 'Agency name required for competitor identification'
            }
        
        properties = dataset.get('properties', [])
        
        if not properties:
            logger.warning(f"No properties in dataset for competitor identification")
            return {
                'error': 'no_data',
                'message': 'Insufficient market data for competitor analysis'
            }
        
        # ========================================
        # STEP 1: CALCULATE MARKET SHARE FOR ALL AGENTS
        # ========================================
        
        agent_stats = {}
        total_listings = 0
        
        for prop in properties:
            agent = prop.get('agent', 'Private')
            
            if agent == 'Private':
                continue
            
            total_listings += 1
            
            if agent not in agent_stats:
                agent_stats[agent] = {
                    'listings': 0,
                    'avg_price': 0,
                    'price_sum': 0,
                    'properties': []
                }
            
            agent_stats[agent]['listings'] += 1
            
            price = prop.get('price', 0)
            if price:
                agent_stats[agent]['price_sum'] += price
            
            agent_stats[agent]['properties'].append(prop)
        
        # Calculate averages and market share
        for agent, stats in agent_stats.items():
            stats['market_share'] = (stats['listings'] / total_listings * 100) if total_listings > 0 else 0
            stats['avg_price'] = (stats['price_sum'] / stats['listings']) if stats['listings'] > 0 else 0
        
        # ========================================
        # STEP 2: FIND CLIENT'S AGENCY (FUZZY MATCH)
        # ========================================
        
        client_agent_key = CompetitorIdentifier._find_client_agent(
            agency_name,
            list(agent_stats.keys())
        )
        
        client_stats = None
        if client_agent_key:
            client_stats = agent_stats.pop(client_agent_key)
            logger.info(f"✅ Found client agency: {client_agent_key} ({client_stats['listings']} listings, {client_stats['market_share']:.1f}% share)")
        else:
            logger.warning(f"⚠️ Client agency '{agency_name}' not found in market data")
        
        # ========================================
        # STEP 3: RANK COMPETITORS BY MARKET SHARE
        # ========================================
        
        competitors = sorted(
            agent_stats.items(),
            key=lambda x: x[1]['market_share'],
            reverse=True
        )[:max_competitors]
        
        # ========================================
        # STEP 4: BUILD COMPETITOR INTELLIGENCE
        # ========================================
        
        competitor_list = []
        
        for agent_name, stats in competitors:
            competitor_list.append({
                'name': agent_name,
                'market_share': round(stats['market_share'], 1),
                'listings': stats['listings'],
                'avg_price': int(stats['avg_price']),
                'positioning': CompetitorIdentifier._determine_positioning(
                    stats['avg_price'],
                    dataset.get('metrics', {}).get('avg_price', 0)
                )
            })
        
        # ========================================
        # STEP 5: POSITIONING ANALYSIS
        # ========================================
        
        market_avg_price = dataset.get('metrics', {}).get('avg_price', 0)
        
        positioning_analysis = None
        if client_stats:
            client_avg = client_stats['avg_price']
            
            if client_avg > market_avg_price * 1.2:
                positioning = "Ultra-premium positioning"
            elif client_avg > market_avg_price:
                positioning = "Premium positioning"
            elif client_avg > market_avg_price * 0.8:
                positioning = "Market-rate positioning"
            else:
                positioning = "Value positioning"
            
            positioning_analysis = {
                'agency': client_agent_key or agency_name,
                'market_share': round(client_stats['market_share'], 1),
                'listings': client_stats['listings'],
                'avg_price': int(client_avg),
                'vs_market': f"+{int((client_avg - market_avg_price) / market_avg_price * 100)}%" if client_avg > market_avg_price else f"{int((client_avg - market_avg_price) / market_avg_price * 100)}%",
                'positioning': positioning
            }
        
        # ========================================
        # STEP 6: THREAT/OPPORTUNITY ASSESSMENT
        # ========================================
        
        threats = []
        opportunities = []
        
        for comp in competitor_list[:3]:
            # Threat: High market share competitor
            if comp['market_share'] > 15:
                threats.append(f"{comp['name']}: Dominant position ({comp['market_share']}% share)")
            
            # Opportunity: Competitor with declining velocity (if available)
            # This would require historical data - placeholder for now
        
        return {
            'client_agency': positioning_analysis,
            'competitors': competitor_list,
            'threats': threats,
            'opportunities': opportunities,
            'market_context': {
                'total_agents': len(agent_stats) + (1 if client_stats else 0),
                'total_listings': total_listings,
                'market_avg_price': int(market_avg_price)
            },
            'timestamp': datetime.now(timezone.utc).isoformat()
        }
    
    @staticmethod
    def _find_client_agent(agency_name: str, agent_list: List[str]) -> Optional[str]:
        """
        Fuzzy match client agency name to agent in dataset
        
        Handles variations like:
        - "Wetherell" → "Wetherell Mayfair"
        - "Knight Frank" → "Knight Frank - Mayfair"
        """
        
        agency_name_lower = agency_name.lower().strip()
        
        # Exact match first
        for agent in agent_list:
            if agent.lower() == agency_name_lower:
                return agent
        
        # Partial match (agency name is substring of agent name)
        for agent in agent_list:
            if agency_name_lower in agent.lower():
                return agent
        
        # Reverse partial match (agent name is substring of agency name)
        for agent in agent_list:
            if agent.lower() in agency_name_lower:
                return agent
        
        # No match found
        return None
    
    @staticmethod
    def _determine_positioning(agent_avg_price: float, market_avg_price: float) -> str:
        """Determine positioning tier relative to market"""
        
        if market_avg_price == 0:
            return "Unknown"
        
        ratio = agent_avg_price / market_avg_price
        
        if ratio > 1.3:
            return "Ultra-premium"
        elif ratio > 1.1:
            return "Premium"
        elif ratio > 0.9:
            return "Market-rate"
        else:
            return "Value"


def get_competitor_intelligence(agency_name: str, dataset: Dict) -> Optional[Dict]:
    """
    Convenience function for getting competitor intelligence
    
    Usage:
        competitor_intel = get_competitor_intelligence("Wetherell", dataset)
    """
    
    if not agency_name or not dataset:
        return None
    
    return CompetitorIdentifier.identify_competitors(agency_name, dataset)
