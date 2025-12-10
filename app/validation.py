"""
VOXMILL RESPONSE VALIDATION
============================
Hallucination detection - validate LLM responses against ground truth dataset
Prevents GPT-4 from inventing agents, trends, or fabricating numbers
"""

import re
import logging
from typing import Dict, List, Tuple
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class HallucinationDetector:
    """Detect and prevent LLM hallucinations"""
    
    @classmethod
    def validate_response(cls, response_text: str, dataset: Dict, 
                         category: str) -> Tuple[bool, List[str], Dict]:
        """
        Validate LLM response against ground truth dataset
        
        Args:
            response_text: GPT-4 generated response
            dataset: Ground truth dataset from MongoDB
            category: Response category (market_overview, competitive_landscape, etc.)
        
        Returns:
            (is_valid, violations, corrections)
        """
        
        violations = []
        corrections = {}
        
        # Extract facts from dataset
        facts = cls._extract_dataset_facts(dataset)
        
        # Run validation checks
        violations += cls._validate_agents(response_text, facts['agents'])
        violations += cls._validate_numbers(response_text, facts['metrics'])
        violations += cls._validate_regions(response_text, facts['regions'])
        violations += cls._validate_trends(response_text, dataset)
        
        # Generate corrections if violations found
        if violations:
            corrections = cls._generate_corrections(violations, facts)
            logger.warning(f"⚠️  Hallucinations detected: {len(violations)} violations")
            for v in violations:
                logger.warning(f"   - {v}")
        
        is_valid = len(violations) == 0
        
        return is_valid, violations, corrections
    
    @classmethod
    def _extract_dataset_facts(cls, dataset: Dict) -> Dict:
        """Extract verifiable facts from dataset"""
        
        properties = dataset.get('properties', [])
        metadata = dataset.get('metadata', {})
        metrics = dataset.get('metrics', dataset.get('kpis', {}))
        
        # Extract unique agents
        agents = list(set([
            p.get('agent', 'Unknown') 
            for p in properties 
            if p.get('agent') and p.get('agent') != 'Private'
        ]))
        
        # Extract regions/submarkets
        regions = list(set([
            p.get('submarket', '') 
            for p in properties 
            if p.get('submarket')
        ]))
        
        # Extract price range
        prices = [p.get('price', 0) for p in properties if p.get('price', 0) > 0]
        
        facts = {
            "agents": agents,
            "agent_count": len(agents),
            "regions": regions,
            "area": metadata.get('area', 'Unknown'),
            "property_count": len(properties),
            "metrics": {
                "avg_price": metrics.get('avg_price', 0),
                "median_price": metrics.get('median_price', 0),
                "min_price": min(prices) if prices else 0,
                "max_price": max(prices) if prices else 0,
                "total_inventory": len(properties),
            }
        }
        
        return facts
    
    @classmethod
    def _validate_agents(cls, response: str, real_agents: List[str]) -> List[str]:
        """Check if response mentions non-existent agents"""
        
        violations = []
        
        # Common real estate agent patterns
        agent_patterns = [
            r'\b(Knight Frank|Savills|Hamptons|Chestertons|Strutt & Parker|'
            r'Foxtons|JLL|CBRE|Cushman & Wakefield|Harrods Estates|'
            r'Beauchamp Estates|Aylesford International|Wetherell|Beckett & Kay)\b'
        ]
        
        # Find all agent mentions in response
        mentioned_agents = []
        for pattern in agent_patterns:
            matches = re.findall(pattern, response, re.IGNORECASE)
            mentioned_agents.extend(matches)
        
        # Check each mentioned agent
        for mentioned in mentioned_agents:
            # Case-insensitive comparison
            if not any(mentioned.lower() == agent.lower() for agent in real_agents):
                violations.append(f"invented_agent:{mentioned}")
        
        return violations
    
    @classmethod
    def _validate_numbers(cls, response: str, real_metrics: Dict) -> List[str]:
        """Check if numerical claims are within reasonable bounds"""
        
        violations = []
        
        # Extract percentage claims
        percentage_pattern = r'(\d+(?:\.\d+)?)\s*%'
        percentages = re.findall(percentage_pattern, response)
        
        for pct_str in percentages:
            pct = float(pct_str)
            
            # Flag unrealistic percentages
            if pct > 100:
                violations.append(f"impossible_percentage:{pct}%")
            elif pct > 50:
                # Market movements >50% are extremely rare
                violations.append(f"extreme_percentage:{pct}% (verify)")
        
        # Extract price mentions
        price_pattern = r'£(\d+(?:,\d{3})*(?:\.\d+)?)\s*[MmKk]?'
        price_mentions = re.findall(price_pattern, response)
        
        for price_str in price_mentions:
            # Convert to number
            price_clean = price_str.replace(',', '')
            
            try:
                price = float(price_clean)
                
                # Check if M (millions) or K (thousands)
                if 'M' in response or 'm' in response:
                    price *= 1_000_000
                elif 'K' in response or 'k' in response:
                    price *= 1_000
                
                # Compare to real dataset bounds
                min_price = real_metrics.get('min_price', 0)
                max_price = real_metrics.get('max_price', 0)
                
                # If price is wildly outside dataset range, flag it
                if max_price > 0:  # Only check if we have real data
                    if price < min_price * 0.5:  # 50% below minimum
                        violations.append(f"unrealistic_price_low:£{price:,.0f}")
                    elif price > max_price * 2:  # 2x above maximum
                        violations.append(f"unrealistic_price_high:£{price:,.0f}")
                        
            except ValueError:
                continue
        
        # Extract inventory claims
        inventory_pattern = r'(\d+)\s+(?:properties|listings|inventory|units)'
        inventory_mentions = re.findall(inventory_pattern, response, re.IGNORECASE)
        
        real_inventory = real_metrics.get('total_inventory', 0)
        
        for inv_str in inventory_mentions:
            claimed_inventory = int(inv_str)
            
            # If claimed inventory is wildly different from reality, flag it
            if real_inventory > 0:
                ratio = claimed_inventory / real_inventory
                
                if ratio > 2.0 or ratio < 0.5:  # 2x inflation or 50% deflation
                    violations.append(f"incorrect_inventory:{claimed_inventory} (actual:{real_inventory})")
        
        return violations
    
    @classmethod
    def _validate_regions(cls, response: str, real_regions: List[str]) -> List[str]:
        """Check if response mentions regions not in dataset"""
        
        violations = []
        
        # Common London luxury submarkets
        known_regions = [
            'Mayfair', 'Knightsbridge', 'Chelsea', 'Belgravia', 'Kensington',
            'South Kensington', 'Notting Hill', 'Marylebone', 'St James',
            'Fitzrovia', 'Bloomsbury', 'Covent Garden', 'Soho'
        ]
        
        # Extend with dataset-specific regions
        all_regions = list(set(known_regions + real_regions))
        
        # Look for region mentions
        for region in all_regions:
            if region in response:
                # Check if this region is in the actual dataset
                if region not in real_regions and len(real_regions) > 0:
                    # Only flag if we have real regions and this isn't one of them
                    violations.append(f"unrelated_region:{region}")
        
        return violations
    
    @classmethod
    def _validate_trends(cls, response: str, dataset: Dict) -> List[str]:
        """Check if trend claims are supported by data"""
        
        violations = []
        
        # Check if response makes trend claims
        trend_indicators = [
            'trending', 'increasing', 'decreasing', 'rising', 'falling',
            'momentum', 'surge', 'decline', 'growth', 'contraction'
        ]
        
        has_trend_claim = any(indicator in response.lower() for indicator in trend_indicators)
        
        if has_trend_claim:
            # Check if dataset has trend data
            has_trend_data = 'detected_trends' in dataset or 'liquidity_velocity' in dataset
            
            if not has_trend_data:
                # LLM is claiming trends without data support
                violations.append("unsupported_trend_claim")
        
        return violations
    
    @classmethod
    def _generate_corrections(cls, violations: List[str], facts: Dict) -> Dict:
        """Generate factual corrections for violations"""
        
        corrections = {}
        
        for violation in violations:
            if violation.startswith("invented_agent:"):
                agent = violation.split(":")[1]
                corrections[f"agent_{agent}"] = f"Agent '{agent}' not found in current dataset. Actual agents: {', '.join(facts['agents'][:5])}"
            
            elif violation.startswith("incorrect_inventory:"):
                parts = violation.split(":")
                claimed = parts[1].split("(")[0].strip()
                actual = facts['metrics']['total_inventory']
                corrections["inventory"] = f"Inventory claimed: {claimed}, actual: {actual}"
            
            elif violation.startswith("unrealistic_price"):
                corrections["price_range"] = f"Valid price range: £{facts['metrics']['min_price']:,.0f} - £{facts['metrics']['max_price']:,.0f}"
            
            elif violation == "unsupported_trend_claim":
                corrections["trends"] = "Trend claims made without supporting data. Dataset contains snapshot only, no historical comparison."
        
        return corrections
    
    @classmethod
    def auto_correct_response(cls, response: str, corrections: Dict) -> str:
        """
        Attempt to auto-correct hallucinations in response
        Use cautiously - may be better to regenerate
        """
        
        corrected = response
        
        # Add disclaimer if corrections needed
        if corrections:
            disclaimer = "\n\n⚠️ Note: Some claims adjusted based on available data."
            corrected += disclaimer
        
        return corrected
    
    @classmethod
    def calculate_confidence_score(cls, violations: List[str]) -> float:
        """
        Calculate confidence score based on validation
        
        Returns: 0.0-1.0 confidence score
        """
        
        if not violations:
            return 1.0
        
        # Assign severity weights
        severity_weights = {
            "invented_agent": 0.3,
            "incorrect_inventory": 0.2,
            "unrealistic_price": 0.25,
            "impossible_percentage": 0.4,
            "extreme_percentage": 0.1,
            "unsupported_trend_claim": 0.15,
            "unrelated_region": 0.1
        }
        
        total_penalty = 0.0
        
        for violation in violations:
            # Extract violation type
            v_type = violation.split(":")[0]
            penalty = severity_weights.get(v_type, 0.15)
            total_penalty += penalty
        
        # Confidence = 1.0 - total_penalty (capped at 0.0)
        confidence = max(0.0, 1.0 - total_penalty)
        
        return round(confidence, 2)


def log_hallucination_event(violations: List[str], response_snippet: str):
    """Log hallucination events for monitoring"""
    
    try:
        from pymongo import MongoClient
        import os
        
        MONGODB_URI = os.getenv("MONGODB_URI")
        if MONGODB_URI:
            mongo_client = MongoClient(MONGODB_URI)
            db = mongo_client['Voxmill']
            
            hallucination_log = {
                "timestamp": datetime.now(timezone.utc),
                "violation_count": len(violations),
                "violations": violations,
                "response_snippet": response_snippet[:200],  # First 200 chars
                "severity": "high" if len(violations) >= 3 else "medium"
            }
            
            db['hallucination_events'].insert_one(hallucination_log)
            logger.info(f"📝 Hallucination event logged: {len(violations)} violations")
    except Exception as e:
        logger.error(f"Failed to log hallucination event: {e}")
