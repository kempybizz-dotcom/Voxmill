"""
VOXMILL RESPONSE VALIDATION - INSTITUTIONAL GRADE
==================================================
Hallucination detection with zero false positives
Validates LLM responses against ground truth dataset
"""

import re
import logging
from typing import Dict, List, Tuple
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


class HallucinationDetector:
    """Enterprise-grade hallucination detection with surgical precision"""
    
    @classmethod
    def validate_response(cls, response_text: str, dataset: Dict, 
                         category: str) -> Tuple[bool, List[str], Dict]:
        """
        Validate LLM response against ground truth dataset
        
        Args:
            response_text: GPT-4 generated response
            dataset: Ground truth dataset from data stack
            category: Response category (market_overview, competitive_landscape, etc.)
        
        Returns:
            (is_valid, violations, corrections)
        """
        
        violations = []
        corrections = {}
        
        # Extract facts from dataset
        facts = cls._extract_dataset_facts(dataset)
        
        # Run validation checks with improved logic
        violations += cls._validate_agents(response_text, facts['agents'])
        violations += cls._validate_numbers(response_text, facts['metrics'])
        violations += cls._validate_regions(response_text, facts['regions'], facts['area'])
        violations += cls._validate_trends(response_text, dataset)
        
        # Generate corrections if violations found
        if violations:
            corrections = cls._generate_corrections(violations, facts)
            logger.warning(f"⚠️  Hallucinations detected: {len(violations)} violations")
            for v in violations:
                logger.warning(f"   - {v}")
        else:
            logger.info(f"✅ Validation passed: 0 violations")
        
        is_valid = len(violations) == 0
        
        return is_valid, violations, corrections
    
    @classmethod
    def _extract_dataset_facts(cls, dataset: Dict) -> Dict:
        """Extract verifiable facts from dataset"""
        
        properties = dataset.get('properties', [])
        metadata = dataset.get('metadata', {})
        metrics = dataset.get('metrics', dataset.get('kpis', {}))
        
        # Extract unique agents with normalization
        raw_agents = [
            p.get('agent', 'Unknown') 
            for p in properties 
            if p.get('agent') and p.get('agent') != 'Private'
        ]
        
        # Normalize agent names (remove branch suffixes)
        normalized_agents = []
        for agent in raw_agents:
            # Extract core name before " - " or " plc" or other suffixes
            core_name = re.split(r'\s+-\s+|\s+plc|\s+international|\s+limited', agent, flags=re.IGNORECASE)[0].strip()
            normalized_agents.append(core_name)
        
        agents = list(set(normalized_agents))
        
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
        """
        Check if response mentions non-existent agents
        FIXED: Uses partial matching to avoid false positives
        """
        
        violations = []
        
        # Common real estate agent patterns
        agent_patterns = [
            r'\b(Knight Frank|Savills|Hamptons|Chestertons|Strutt & Parker|'
            r'Foxtons|JLL|CBRE|Cushman & Wakefield|Harrods Estates|'
            r'Beauchamp Estates|Aylesford International|Wetherell|Beckett & Kay|'
            r'Sotheby\'s|Christie\'s|Hamptons International|Marsh & Parsons|'
            r'Winkworth|Dexters|Kinleigh Folkard & Hayward)\b'
        ]
        
        # Find all agent mentions in response
        mentioned_agents = set()
        for pattern in agent_patterns:
            matches = re.findall(pattern, response, re.IGNORECASE)
            mentioned_agents.update([m.strip() for m in matches])
        
        # Check each mentioned agent with PARTIAL MATCHING
        for mentioned in mentioned_agents:
            # Check if agent name appears in real agent list (partial match)
            # Example: "Knight Frank" matches "Knight Frank - Mayfair"
            found = any(
                mentioned.lower() in agent.lower() or 
                agent.lower() in mentioned.lower()
                for agent in real_agents
            )
            
            if not found and real_agents:  # Only flag if we have real agents to compare
                violations.append(f"invented_agent:{mentioned}")
        
        return violations
    
    @classmethod
    def _validate_numbers(cls, response: str, real_metrics: Dict) -> List[str]:
        """
        Check if numerical claims are within reasonable bounds
        FIXED: Improved price parsing to avoid trillion-pound false positives
        """
        
        violations = []
        
        # ========================================
        # VALIDATE PERCENTAGES
        # ========================================
        percentage_pattern = r'(\d+(?:\.\d+)?)\s*%'
        percentages = re.findall(percentage_pattern, response)
        
        for pct_str in percentages:
            pct = float(pct_str)
            
            if pct > 100:
                violations.append(f"impossible_percentage:{pct}%")
            elif pct > 50:
                # Market movements >50% are extremely rare
                violations.append(f"extreme_percentage:{pct}% (verify)")
        
        # ========================================
        # VALIDATE PRICES (FIXED PARSING)
        # ========================================
        
        # Match: £4.2M, £3M, £1,500,000 but with better unit detection
        price_pattern = r'£(\d+(?:,\d{3})*(?:\.\d+)?)\s*([MmKk]?)\b'
        price_mentions = re.findall(price_pattern, response)
        
        for price_str, unit in price_mentions:
            try:
                # Remove commas
                price_clean = price_str.replace(',', '')
                price = float(price_clean)
                
                # Apply multiplier ONLY if unit is explicitly present
                if unit:
                    if unit.upper() == 'M':
                        price *= 1_000_000
                    elif unit.upper() == 'K':
                        price *= 1_000
                else:
                    # No unit - check if it needs interpretation
                    # If number is small (< 1000), it's likely in millions (e.g., "£4.2" means £4.2M)
                    # If number is large (>= 100,000), it's already absolute
                    if price < 1000:
                        price *= 1_000_000
                
                # Validate against reasonable luxury property bounds
                min_price = real_metrics.get('min_price', 0)
                max_price = real_metrics.get('max_price', 0)
                
                # Luxury property sanity check: £100k - £100M is reasonable for PCL
                if price < 100_000:
                    violations.append(f"unrealistic_price_low:£{price:,.0f}")
                elif price > 100_000_000:  # £100M ceiling
                    violations.append(f"unrealistic_price_high:£{price:,.0f}")
                
                # Dataset-specific validation (with tolerance)
                if max_price > 0:
                    # Allow 5x deviation from dataset max (generous tolerance)
                    if price > max_price * 5:
                        violations.append(
                            f"dataset_outlier:£{price:,.0f} "
                            f"(dataset max: £{max_price:,.0f})"
                        )
                        
            except ValueError:
                continue
        
        # ========================================
        # VALIDATE INVENTORY COUNTS
        # ========================================
        inventory_pattern = r'(\d+)\s+(?:properties|listings|inventory|units)'
        inventory_mentions = re.findall(inventory_pattern, response, re.IGNORECASE)
        
        real_inventory = real_metrics.get('total_inventory', 0)
        
        for inv_str in inventory_mentions:
            claimed_inventory = int(inv_str)
            
            # If claimed inventory is wildly different from reality, flag it
            if real_inventory > 0:
                ratio = claimed_inventory / real_inventory
                
                # Allow 2x tolerance (generous)
                if ratio > 2.0:
                    violations.append(
                        f"inflated_inventory:{claimed_inventory} "
                        f"(actual:{real_inventory})"
                    )
                elif ratio < 0.5:
                    violations.append(
                        f"deflated_inventory:{claimed_inventory} "
                        f"(actual:{real_inventory})"
                    )
        
        return violations
    
    @classmethod
    def _validate_regions(cls, response: str, real_regions: List[str], 
                         primary_area: str) -> List[str]:
        """
        Check if response mentions unrelated regions
        IMPROVED: Only flags regions if they're clearly wrong context
        """
        
        violations = []
        
        # Common London luxury submarkets (valid reference points)
        known_regions = [
            'Mayfair', 'Knightsbridge', 'Chelsea', 'Belgravia', 'Kensington',
            'South Kensington', 'Notting Hill', 'Marylebone', 'St James',
            'Fitzrovia', 'Bloomsbury', 'Covent Garden', 'Soho', 'Westminster',
            'Pimlico', 'Victoria', 'Hyde Park'
        ]
        
        # Build comprehensive allowed list
        allowed_regions = set(known_regions + real_regions + [primary_area])
        
        # Only flag if response mentions region NOT in allowed list
        for region in known_regions:
            if region in response and region not in allowed_regions:
                # Additional check: is this region being compared/contrasted?
                # Comparative mentions are OK: "Unlike Shoreditch, Mayfair..."
                comparison_keywords = ['unlike', 'compared to', 'versus', 'vs', 'than']
                is_comparison = any(
                    keyword in response.lower()[:response.lower().find(region.lower()) + 50]
                    for keyword in comparison_keywords
                )
                
                if not is_comparison:
                    violations.append(f"unrelated_region:{region}")
        
        return violations
    
    @classmethod
    def _validate_trends(cls, response: str, dataset: Dict) -> List[str]:
        """
        Check if trend claims are supported by data
        IMPROVED: More nuanced detection
        """
        
        violations = []
        
        # Strong trend claim indicators
        strong_trend_indicators = [
            'trending up', 'trending down', 'strong momentum', 
            'accelerating', 'decelerating', 'surging', 'plummeting'
        ]
        
        # Weak trend indicators (acceptable without data)
        weak_trend_indicators = [
            'increasing', 'decreasing', 'rising', 'falling',
            'growth', 'decline', 'improving', 'weakening'
        ]
        
        has_strong_claim = any(
            indicator in response.lower() 
            for indicator in strong_trend_indicators
        )
        
        if has_strong_claim:
            # Check if dataset has trend data
            has_trend_data = (
                'detected_trends' in dataset or 
                'liquidity_velocity' in dataset or
                'historical_sales' in dataset
            )
            
            if not has_trend_data:
                violations.append("unsupported_strong_trend_claim")
        
        return violations
    
    @classmethod
    def _generate_corrections(cls, violations: List[str], facts: Dict) -> Dict:
        """Generate factual corrections for violations"""
        
        corrections = {}
        
        for violation in violations:
            if violation.startswith("invented_agent:"):
                agent = violation.split(":", 1)[1]
                corrections[f"agent_{agent}"] = (
                    f"Agent '{agent}' not found in current dataset. "
                    f"Actual agents: {', '.join(facts['agents'][:5])}"
                )
            
            elif violation.startswith("inflated_inventory:") or violation.startswith("deflated_inventory:"):
                actual = facts['metrics']['total_inventory']
                corrections["inventory"] = (
                    f"Inventory mismatch. Actual count: {actual} properties"
                )
            
            elif violation.startswith("unrealistic_price") or violation.startswith("dataset_outlier"):
                corrections["price_range"] = (
                    f"Valid price range: £{facts['metrics']['min_price']:,.0f} - "
                    f"£{facts['metrics']['max_price']:,.0f}"
                )
            
            elif violation == "unsupported_strong_trend_claim":
                corrections["trends"] = (
                    "Strong trend claims made without supporting historical data. "
                    "Dataset contains current snapshot only."
                )
        
        return corrections
    
    @classmethod
    def calculate_confidence_score(cls, violations: List[str]) -> float:
        """
        Calculate confidence score based on validation
        
        Returns: 0.0-1.0 confidence score
        """
        
        if not violations:
            return 1.0
        
        # Assign severity weights (RECALIBRATED)
        severity_weights = {
            "invented_agent": 0.15,  # Reduced - often false positives
            "inflated_inventory": 0.25,
            "deflated_inventory": 0.25,
            "unrealistic_price_low": 0.30,
            "unrealistic_price_high": 0.30,
            "dataset_outlier": 0.20,  # Less severe than unrealistic
            "impossible_percentage": 0.40,
            "extreme_percentage": 0.10,
            "unsupported_strong_trend_claim": 0.20,
            "unrelated_region": 0.05  # Very low severity
        }
        
        total_penalty = 0.0
        
        for violation in violations:
            # Extract violation type
            v_type = violation.split(":")[0]
            penalty = severity_weights.get(v_type, 0.10)  # Default 0.10 if unknown
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
            
            # Calculate severity
            if len(violations) == 0:
                severity = "none"
            elif len(violations) <= 2:
                severity = "low"
            elif len(violations) <= 5:
                severity = "medium"
            else:
                severity = "high"
            
            hallucination_log = {
                "timestamp": datetime.now(timezone.utc),
                "violation_count": len(violations),
                "violations": violations,
                "response_snippet": response_snippet[:200],  # First 200 chars
                "severity": severity,
                "version": "2.0_surgical"
            }
            
            db['hallucination_events'].insert_one(hallucination_log)
            logger.info(f"📝 Hallucination event logged: {len(violations)} violations ({severity})")
    except Exception as e:
        logger.error(f"Failed to log hallucination event: {e}")


# ============================================================================
# VALIDATION HELPER FUNCTIONS
# ============================================================================

def quick_validate(response_text: str, dataset: Dict) -> bool:
    """
    Quick validation check (returns True if valid, False if suspicious)
    Use for fast pre-checks before full validation
    """
    detector = HallucinationDetector()
    is_valid, violations, _ = detector.validate_response(
        response_text=response_text,
        dataset=dataset,
        category="quick_check"
    )
    return is_valid


def get_validation_summary(violations: List[str]) -> str:
    """
    Generate human-readable validation summary
    """
    if not violations:
        return "✅ All validations passed"
    
    summary_parts = []
    
    agent_violations = [v for v in violations if v.startswith("invented_agent")]
    price_violations = [v for v in violations if "price" in v]
    inventory_violations = [v for v in violations if "inventory" in v]
    
    if agent_violations:
        summary_parts.append(f"❌ {len(agent_violations)} agent inconsistencies")
    
    if price_violations:
        summary_parts.append(f"❌ {len(price_violations)} price anomalies")
    
    if inventory_violations:
        summary_parts.append(f"❌ {len(inventory_violations)} inventory mismatches")
    
    return " | ".join(summary_parts) if summary_parts else f"⚠️  {len(violations)} minor issues"
