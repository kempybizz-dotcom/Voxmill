"""
VOXMILL MULTI-INDUSTRY MOCK DATA GENERATOR (PRODUCTION-SAFE EDITION)
====================================================================
Production-grade synthetic data for testing and demos

✅ CRITICAL FIX: ALL real names removed
- NO real agency names
- NO real brand names  
- NO real clinic names
- NO real venue names
- ALL data obviously synthetic

Industries Supported:
- Real Estate (London luxury markets)
- Automotive (Prestige car dealerships)
- Healthcare (Private clinics)
- Hospitality (Premium hotels/restaurants)

Features:
- Realistic pricing distributions
- Competitor presence simulation
- Daily variance for freshness
- Industry-specific attributes
- SYNTHETIC FLAG on all data
"""

import random
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List
import hashlib
import statistics

# ============================================================
# INDUSTRY: REAL ESTATE
# ============================================================

class RealEstateMockData:
    """Generate realistic property data with SYNTHETIC names only"""
    
    # ✅ SYNTHETIC AGENCIES (NO REAL NAMES)
    AGENTS = [
        ('Mock Agency Alpha', 0.18),
        ('Mock Agency Beta', 0.15),
        ('Mock Agency Gamma', 0.14),
        ('Mock Agency Delta', 0.12),
        ('Mock Agency Epsilon', 0.10),
        ('Mock Agency Zeta', 0.09),
        ('Mock Agency Eta', 0.08),
        ('Mock Agency Theta', 0.07),
        ('Mock Agency Iota', 0.04),
        ('Demo Listing Service', 0.03)
    ]
    
    PROPERTY_TYPES = [
        ('Apartment', 0.45),
        ('Penthouse', 0.15),
        ('Townhouse', 0.20),
        ('Mews House', 0.12),
        ('Mansion', 0.08)
    ]
    
    # ✅ SYNTHETIC STREETS (obviously fake)
    STREETS = [
        'Synthetic Avenue A', 'Synthetic Avenue B', 'Synthetic Avenue C',
        'Demo Square North', 'Demo Square South', 'Demo Square East',
        'Mock Boulevard 1', 'Mock Boulevard 2', 'Mock Boulevard 3',
        'Test Street Alpha', 'Test Street Beta', 'Test Street Gamma',
        'Example Lane A', 'Example Lane B', 'Example Lane C'
    ]
    
    @staticmethod
    def generate_properties(area: str, count: int = 75) -> List[Dict]:
        """Generate synthetic property listings"""
        
        properties = []
        base_seed = int(hashlib.md5(f"{area}{datetime.now().date()}".encode()).hexdigest(), 16)
        random.seed(base_seed)
        
        # Generic market parameters (not location-specific)
        default_params = {
            'avg_price': 2500000,
            'std_dev': 1000000,
            'avg_sqft': 2000
        }
        
        params = default_params  # All areas use same params in demo mode
        
        for i in range(count):
            # Select agent based on market share
            agent = random.choices(
                [a[0] for a in RealEstateMockData.AGENTS],
                weights=[a[1] for a in RealEstateMockData.AGENTS]
            )[0]
            
            # Select property type
            prop_type = random.choices(
                [t[0] for t in RealEstateMockData.PROPERTY_TYPES],
                weights=[t[1] for t in RealEstateMockData.PROPERTY_TYPES]
            )[0]
            
            # Generate price (normal distribution)
            price = max(500000, int(random.gauss(params['avg_price'], params['std_dev'])))
            price = round(price / 50000) * 50000
            
            # Generate size
            size_sqft = max(600, int(random.gauss(params['avg_sqft'], 400)))
            price_per_sqft = round(price / size_sqft, 2)
            
            # Bedrooms based on size
            if size_sqft < 1000:
                bedrooms = random.choice([1, 2])
            elif size_sqft < 1800:
                bedrooms = random.choice([2, 3])
            elif size_sqft < 2500:
                bedrooms = random.choice([3, 4])
            else:
                bedrooms = random.choice([4, 5, 6])
            
            # Days on market
            days_on_market = max(1, int(random.expovariate(1/45)))
            
            # Address with synthetic street
            street = random.choice(RealEstateMockData.STREETS)
            number = random.randint(1, 150)
            address = f"{number} {street}, {area}"
            
            # Status
            status = 'active' if days_on_market < 90 else random.choice(['active', 'under_offer'])
            
            properties.append({
                'id': f"mock_re_{area.lower()}_{i}_{int(time.time())}",
                'price': price,
                'bedrooms': bedrooms,
                'property_type': prop_type,
                'size_sqft': size_sqft,
                'price_per_sqft': price_per_sqft,
                'agent': agent,
                'address': address,
                'area': area,
                'submarket': area,
                'days_on_market': days_on_market,
                'status': status,
                'source': 'synthetic_demo',  # ← Changed from 'mock_data'
                'is_synthetic': True,  # ← FLAG
                'scraped_at': datetime.now(timezone.utc).isoformat()
            })
        
        return properties


# ============================================================
# INDUSTRY: AUTOMOTIVE
# ============================================================

class AutomotiveMockData:
    """Generate synthetic car dealership data"""
    
    # ✅ SYNTHETIC DEALERS (NO REAL NAMES)
    DEALERS = [
        ('Mock Dealership Alpha', 0.22),
        ('Mock Dealership Beta', 0.18),
        ('Mock Dealership Gamma', 0.15),
        ('Mock Dealership Delta', 0.12),
        ('Mock Dealership Epsilon', 0.10),
        ('Mock Dealership Zeta', 0.08),
        ('Mock Dealership Eta', 0.07),
        ('Demo Auto Group', 0.05),
        ('Private Sale', 0.03)
    ]
    
    # ✅ SYNTHETIC BRANDS (obviously fake)
    BRANDS = [
        ('Mock Brand Luxury A', 0.20),
        ('Mock Brand Luxury B', 0.18),
        ('Mock Brand Luxury C', 0.15),
        ('Mock Brand Luxury D', 0.12),
        ('Mock Brand Luxury E', 0.10),
        ('Mock Brand Luxury F', 0.10),
        ('Mock Brand Luxury G', 0.08),
        ('Mock Brand Luxury H', 0.05),
        ('Mock Brand Luxury I', 0.02)
    ]
    
    MODELS = {
        'Mock Brand Luxury A': ['Model X1', 'Model X2', 'Model X3'],
        'Mock Brand Luxury B': ['Model Y1', 'Model Y2', 'Model Y3', 'Model Y4'],
        'Mock Brand Luxury C': ['Model Z1', 'Model Z2', 'Model Z3', 'Model Z4'],
        'Mock Brand Luxury D': ['Model A1', 'Model A2', 'Model A3'],
        'Mock Brand Luxury E': ['Model B1', 'Model B2', 'Model B3'],
        'Mock Brand Luxury F': ['Model C1', 'Model C2', 'Model C3'],
        'Mock Brand Luxury G': ['Model D1', 'Model D2', 'Model D3'],
        'Mock Brand Luxury H': ['Model E1', 'Model E2', 'Model E3'],
        'Mock Brand Luxury I': ['Model F1', 'Model F2']
    }
    
    @staticmethod
    def generate_inventory(area: str, count: int = 60) -> List[Dict]:
        """Generate synthetic car inventory"""
        
        inventory = []
        base_seed = int(hashlib.md5(f"auto_{area}{datetime.now().date()}".encode()).hexdigest(), 16)
        random.seed(base_seed)
        
        for i in range(count):
            # Select dealer
            dealer = random.choices(
                [d[0] for d in AutomotiveMockData.DEALERS],
                weights=[d[1] for d in AutomotiveMockData.DEALERS]
            )[0]
            
            # Select brand
            brand = random.choices(
                [b[0] for b in AutomotiveMockData.BRANDS],
                weights=[b[1] for b in AutomotiveMockData.BRANDS]
            )[0]
            
            # Select model
            model = random.choice(AutomotiveMockData.MODELS[brand])
            
            # Generic price ranges
            price = random.randint(100000, 500000)
            price = round(price / 5000) * 5000
            
            # Year and mileage
            year = random.randint(2020, 2024)
            mileage = random.randint(500, 15000) if year >= 2023 else random.randint(5000, 35000)
            
            # Days on lot
            days_on_lot = max(1, int(random.expovariate(1/30)))
            
            inventory.append({
                'id': f"mock_auto_{area.lower()}_{i}_{int(time.time())}",
                'price': price,
                'brand': brand,
                'model': model,
                'year': year,
                'mileage': mileage,
                'dealer': dealer,
                'location': area,
                'days_on_lot': days_on_lot,
                'condition': 'Excellent' if mileage < 10000 else 'Good',
                'source': 'synthetic_demo',
                'is_synthetic': True,
                'scraped_at': datetime.now(timezone.utc).isoformat()
            })
        
        return inventory


# ============================================================
# INDUSTRY: HEALTHCARE
# ============================================================

class HealthcareMockData:
    """Generate synthetic healthcare clinic data"""
    
    # ✅ SYNTHETIC CLINICS (NO REAL NAMES)
    CLINICS = [
        ('Mock Medical Center A', 0.20),
        ('Mock Medical Center B', 0.18),
        ('Mock Medical Center C', 0.15),
        ('Mock Medical Center D', 0.12),
        ('Mock Medical Center E', 0.10),
        ('Demo Health Clinic A', 0.08),
        ('Demo Health Clinic B', 0.08),
        ('Demo Health Clinic C', 0.06),
        ('Private Practice', 0.03)
    ]
    
    SPECIALTIES = [
        ('Cardiology', 0.18), ('Orthopedics', 0.16), ('Oncology', 0.14),
        ('Neurology', 0.12), ('Dermatology', 0.10), ('Ophthalmology', 0.10),
        ('Plastic Surgery', 0.08), ('ENT', 0.07), ('Gastroenterology', 0.05)
    ]
    
    @staticmethod
    def generate_services(area: str, count: int = 50) -> List[Dict]:
        """Generate synthetic healthcare service data"""
        
        services = []
        base_seed = int(hashlib.md5(f"health_{area}{datetime.now().date()}".encode()).hexdigest(), 16)
        random.seed(base_seed)
        
        for i in range(count):
            clinic = random.choices(
                [c[0] for c in HealthcareMockData.CLINICS],
                weights=[c[1] for c in HealthcareMockData.CLINICS]
            )[0]
            
            specialty = random.choices(
                [s[0] for s in HealthcareMockData.SPECIALTIES],
                weights=[s[1] for s in HealthcareMockData.SPECIALTIES]
            )[0]
            
            # Generic pricing
            consultation_fee = random.randint(150, 800)
            
            # Wait times
            wait_time = max(1, int(random.expovariate(1/14)))
            
            services.append({
                'id': f"mock_health_{area.lower()}_{i}_{int(time.time())}",
                'clinic': clinic,
                'specialty': specialty,
                'consultation_fee': consultation_fee,
                'wait_time_days': wait_time,
                'location': area,
                'rating': round(random.uniform(4.0, 5.0), 1),
                'accepts_insurance': random.choice([True, True, False]),
                'source': 'synthetic_demo',
                'is_synthetic': True,
                'scraped_at': datetime.now(timezone.utc).isoformat()
            })
        
        return services


# ============================================================
# INDUSTRY: HOSPITALITY
# ============================================================

class HospitalityMockData:
    """Generate synthetic hospitality venue data"""
    
    # ✅ SYNTHETIC VENUES (NO REAL NAMES)
    HOTELS = [
        ('Mock Hotel A', 0.15),
        ('Mock Hotel B', 0.14),
        ('Mock Hotel C', 0.13),
        ('Mock Hotel D', 0.12),
        ('Mock Hotel E', 0.11),
        ('Demo Resort A', 0.10),
        ('Demo Resort B', 0.09),
        ('Demo Resort C', 0.08),
        ('Demo Resort D', 0.08)
    ]
    
    RESTAURANTS = [
        ('Mock Restaurant A', 0.12),
        ('Mock Restaurant B', 0.11),
        ('Mock Restaurant C', 0.10),
        ('Mock Restaurant D', 0.09),
        ('Mock Restaurant E', 0.08),
        ('Demo Dining A', 0.08),
        ('Demo Dining B', 0.07),
        ('Demo Dining C', 0.06),
        ('Demo Dining D', 0.06)
    ]
    
    @staticmethod
    def generate_venues(area: str, count: int = 40) -> List[Dict]:
        """Generate synthetic hospitality venue data"""
        
        venues = []
        base_seed = int(hashlib.md5(f"hosp_{area}{datetime.now().date()}".encode()).hexdigest(), 16)
        random.seed(base_seed)
        
        for i in range(count):
            venue_type = random.choice(['hotel', 'restaurant'])
            
            if venue_type == 'hotel':
                name = random.choices(
                    [h[0] for h in HospitalityMockData.HOTELS],
                    weights=[h[1] for h in HospitalityMockData.HOTELS]
                )[0]
                avg_rate = random.randint(200, 1000)
                occupancy = round(random.uniform(0.65, 0.95), 2)
                
                venues.append({
                    'id': f"mock_hosp_{area.lower()}_{i}_{int(time.time())}",
                    'name': name,
                    'type': 'hotel',
                    'avg_room_rate': avg_rate,
                    'occupancy_rate': occupancy,
                    'location': area,
                    'rating': round(random.uniform(4.3, 5.0), 1),
                    'rooms': random.choice([150, 200, 250, 300]),
                    'source': 'synthetic_demo',
                    'is_synthetic': True,
                    'scraped_at': datetime.now(timezone.utc).isoformat()
                })
            else:
                name = random.choices(
                    [r[0] for r in HospitalityMockData.RESTAURANTS],
                    weights=[r[1] for r in HospitalityMockData.RESTAURANTS]
                )[0]
                avg_check = random.randint(50, 300)
                covers_per_day = random.randint(40, 120)
                
                venues.append({
                    'id': f"mock_hosp_{area.lower()}_{i}_{int(time.time())}",
                    'name': name,
                    'type': 'restaurant',
                    'avg_check': avg_check,
                    'covers_per_day': covers_per_day,
                    'location': area,
                    'rating': round(random.uniform(4.0, 5.0), 1),
                    'michelin_stars': random.choice([0, 1, 1, 2, 3]),
                    'source': 'synthetic_demo',
                    'is_synthetic': True,
                    'scraped_at': datetime.now(timezone.utc).isoformat()
                })
        
        return venues


# ============================================================
# UNIFIED MOCK DATA LOADER
# ============================================================

def load_mock_dataset(area: str, industry: str, max_items: int = 100) -> List[Dict]:
    """
    Load synthetic demo data for any industry
    
    Args:
        area: Geographic area/market
        industry: Industry code (real_estate, automotive, healthcare, hospitality)
        max_items: Maximum items to generate
    
    Returns:
        List of synthetic data items (all flagged with is_synthetic=True)
    """
    
    if industry == "real_estate":
        return RealEstateMockData.generate_properties(area, count=max_items)
    
    elif industry == "automotive":
        return AutomotiveMockData.generate_inventory(area, count=max_items)
    
    elif industry == "healthcare":
        return HealthcareMockData.generate_services(area, count=max_items)
    
    elif industry == "hospitality":
        return HospitalityMockData.generate_venues(area, count=max_items)
    
    else:
        # Default to real estate
        return RealEstateMockData.generate_properties(area, count=max_items)"""
VOXMILL MULTI-INDUSTRY MOCK DATA GENERATOR (PRODUCTION-SAFE EDITION)
====================================================================
Production-grade synthetic data for testing and demos

✅ CRITICAL FIX: ALL real names removed
- NO real agency names
- NO real brand names  
- NO real clinic names
- NO real venue names
- ALL data obviously synthetic

Industries Supported:
- Real Estate (London luxury markets)
- Automotive (Prestige car dealerships)
- Healthcare (Private clinics)
- Hospitality (Premium hotels/restaurants)

Features:
- Realistic pricing distributions
- Competitor presence simulation
- Daily variance for freshness
- Industry-specific attributes
- SYNTHETIC FLAG on all data
"""

import random
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List
import hashlib
import statistics

# ============================================================
# INDUSTRY: REAL ESTATE
# ============================================================

class RealEstateMockData:
    """Generate realistic property data with SYNTHETIC names only"""
    
    # ✅ SYNTHETIC AGENCIES (NO REAL NAMES)
    AGENTS = [
        ('Mock Agency Alpha', 0.18),
        ('Mock Agency Beta', 0.15),
        ('Mock Agency Gamma', 0.14),
        ('Mock Agency Delta', 0.12),
        ('Mock Agency Epsilon', 0.10),
        ('Mock Agency Zeta', 0.09),
        ('Mock Agency Eta', 0.08),
        ('Mock Agency Theta', 0.07),
        ('Mock Agency Iota', 0.04),
        ('Demo Listing Service', 0.03)
    ]
    
    PROPERTY_TYPES = [
        ('Apartment', 0.45),
        ('Penthouse', 0.15),
        ('Townhouse', 0.20),
        ('Mews House', 0.12),
        ('Mansion', 0.08)
    ]
    
    # ✅ SYNTHETIC STREETS (obviously fake)
    STREETS = [
        'Synthetic Avenue A', 'Synthetic Avenue B', 'Synthetic Avenue C',
        'Demo Square North', 'Demo Square South', 'Demo Square East',
        'Mock Boulevard 1', 'Mock Boulevard 2', 'Mock Boulevard 3',
        'Test Street Alpha', 'Test Street Beta', 'Test Street Gamma',
        'Example Lane A', 'Example Lane B', 'Example Lane C'
    ]
    
    @staticmethod
    def generate_properties(area: str, count: int = 75) -> List[Dict]:
        """Generate synthetic property listings"""
        
        properties = []
        base_seed = int(hashlib.md5(f"{area}{datetime.now().date()}".encode()).hexdigest(), 16)
        random.seed(base_seed)
        
        # Generic market parameters (not location-specific)
        default_params = {
            'avg_price': 2500000,
            'std_dev': 1000000,
            'avg_sqft': 2000
        }
        
        params = default_params  # All areas use same params in demo mode
        
        for i in range(count):
            # Select agent based on market share
            agent = random.choices(
                [a[0] for a in RealEstateMockData.AGENTS],
                weights=[a[1] for a in RealEstateMockData.AGENTS]
            )[0]
            
            # Select property type
            prop_type = random.choices(
                [t[0] for t in RealEstateMockData.PROPERTY_TYPES],
                weights=[t[1] for t in RealEstateMockData.PROPERTY_TYPES]
            )[0]
            
            # Generate price (normal distribution)
            price = max(500000, int(random.gauss(params['avg_price'], params['std_dev'])))
            price = round(price / 50000) * 50000
            
            # Generate size
            size_sqft = max(600, int(random.gauss(params['avg_sqft'], 400)))
            price_per_sqft = round(price / size_sqft, 2)
            
            # Bedrooms based on size
            if size_sqft < 1000:
                bedrooms = random.choice([1, 2])
            elif size_sqft < 1800:
                bedrooms = random.choice([2, 3])
            elif size_sqft < 2500:
                bedrooms = random.choice([3, 4])
            else:
                bedrooms = random.choice([4, 5, 6])
            
            # Days on market
            days_on_market = max(1, int(random.expovariate(1/45)))
            
            # Address with synthetic street
            street = random.choice(RealEstateMockData.STREETS)
            number = random.randint(1, 150)
            address = f"{number} {street}, {area}"
            
            # Status
            status = 'active' if days_on_market < 90 else random.choice(['active', 'under_offer'])
            
            properties.append({
                'id': f"mock_re_{area.lower()}_{i}_{int(time.time())}",
                'price': price,
                'bedrooms': bedrooms,
                'property_type': prop_type,
                'size_sqft': size_sqft,
                'price_per_sqft': price_per_sqft,
                'agent': agent,
                'address': address,
                'area': area,
                'submarket': area,
                'days_on_market': days_on_market,
                'status': status,
                'source': 'synthetic_demo',  # ← Changed from 'mock_data'
                'is_synthetic': True,  # ← FLAG
                'scraped_at': datetime.now(timezone.utc).isoformat()
            })
        
        return properties


# ============================================================
# INDUSTRY: AUTOMOTIVE
# ============================================================

class AutomotiveMockData:
    """Generate synthetic car dealership data"""
    
    # ✅ SYNTHETIC DEALERS (NO REAL NAMES)
    DEALERS = [
        ('Mock Dealership Alpha', 0.22),
        ('Mock Dealership Beta', 0.18),
        ('Mock Dealership Gamma', 0.15),
        ('Mock Dealership Delta', 0.12),
        ('Mock Dealership Epsilon', 0.10),
        ('Mock Dealership Zeta', 0.08),
        ('Mock Dealership Eta', 0.07),
        ('Demo Auto Group', 0.05),
        ('Private Sale', 0.03)
    ]
    
    # ✅ SYNTHETIC BRANDS (obviously fake)
    BRANDS = [
        ('Mock Brand Luxury A', 0.20),
        ('Mock Brand Luxury B', 0.18),
        ('Mock Brand Luxury C', 0.15),
        ('Mock Brand Luxury D', 0.12),
        ('Mock Brand Luxury E', 0.10),
        ('Mock Brand Luxury F', 0.10),
        ('Mock Brand Luxury G', 0.08),
        ('Mock Brand Luxury H', 0.05),
        ('Mock Brand Luxury I', 0.02)
    ]
    
    MODELS = {
        'Mock Brand Luxury A': ['Model X1', 'Model X2', 'Model X3'],
        'Mock Brand Luxury B': ['Model Y1', 'Model Y2', 'Model Y3', 'Model Y4'],
        'Mock Brand Luxury C': ['Model Z1', 'Model Z2', 'Model Z3', 'Model Z4'],
        'Mock Brand Luxury D': ['Model A1', 'Model A2', 'Model A3'],
        'Mock Brand Luxury E': ['Model B1', 'Model B2', 'Model B3'],
        'Mock Brand Luxury F': ['Model C1', 'Model C2', 'Model C3'],
        'Mock Brand Luxury G': ['Model D1', 'Model D2', 'Model D3'],
        'Mock Brand Luxury H': ['Model E1', 'Model E2', 'Model E3'],
        'Mock Brand Luxury I': ['Model F1', 'Model F2']
    }
    
    @staticmethod
    def generate_inventory(area: str, count: int = 60) -> List[Dict]:
        """Generate synthetic car inventory"""
        
        inventory = []
        base_seed = int(hashlib.md5(f"auto_{area}{datetime.now().date()}".encode()).hexdigest(), 16)
        random.seed(base_seed)
        
        for i in range(count):
            # Select dealer
            dealer = random.choices(
                [d[0] for d in AutomotiveMockData.DEALERS],
                weights=[d[1] for d in AutomotiveMockData.DEALERS]
            )[0]
            
            # Select brand
            brand = random.choices(
                [b[0] for b in AutomotiveMockData.BRANDS],
                weights=[b[1] for b in AutomotiveMockData.BRANDS]
            )[0]
            
            # Select model
            model = random.choice(AutomotiveMockData.MODELS[brand])
            
            # Generic price ranges
            price = random.randint(100000, 500000)
            price = round(price / 5000) * 5000
            
            # Year and mileage
            year = random.randint(2020, 2024)
            mileage = random.randint(500, 15000) if year >= 2023 else random.randint(5000, 35000)
            
            # Days on lot
            days_on_lot = max(1, int(random.expovariate(1/30)))
            
            inventory.append({
                'id': f"mock_auto_{area.lower()}_{i}_{int(time.time())}",
                'price': price,
                'brand': brand,
                'model': model,
                'year': year,
                'mileage': mileage,
                'dealer': dealer,
                'location': area,
                'days_on_lot': days_on_lot,
                'condition': 'Excellent' if mileage < 10000 else 'Good',
                'source': 'synthetic_demo',
                'is_synthetic': True,
                'scraped_at': datetime.now(timezone.utc).isoformat()
            })
        
        return inventory


# ============================================================
# INDUSTRY: HEALTHCARE
# ============================================================

class HealthcareMockData:
    """Generate synthetic healthcare clinic data"""
    
    # ✅ SYNTHETIC CLINICS (NO REAL NAMES)
    CLINICS = [
        ('Mock Medical Center A', 0.20),
        ('Mock Medical Center B', 0.18),
        ('Mock Medical Center C', 0.15),
        ('Mock Medical Center D', 0.12),
        ('Mock Medical Center E', 0.10),
        ('Demo Health Clinic A', 0.08),
        ('Demo Health Clinic B', 0.08),
        ('Demo Health Clinic C', 0.06),
        ('Private Practice', 0.03)
    ]
    
    SPECIALTIES = [
        ('Cardiology', 0.18), ('Orthopedics', 0.16), ('Oncology', 0.14),
        ('Neurology', 0.12), ('Dermatology', 0.10), ('Ophthalmology', 0.10),
        ('Plastic Surgery', 0.08), ('ENT', 0.07), ('Gastroenterology', 0.05)
    ]
    
    @staticmethod
    def generate_services(area: str, count: int = 50) -> List[Dict]:
        """Generate synthetic healthcare service data"""
        
        services = []
        base_seed = int(hashlib.md5(f"health_{area}{datetime.now().date()}".encode()).hexdigest(), 16)
        random.seed(base_seed)
        
        for i in range(count):
            clinic = random.choices(
                [c[0] for c in HealthcareMockData.CLINICS],
                weights=[c[1] for c in HealthcareMockData.CLINICS]
            )[0]
            
            specialty = random.choices(
                [s[0] for s in HealthcareMockData.SPECIALTIES],
                weights=[s[1] for s in HealthcareMockData.SPECIALTIES]
            )[0]
            
            # Generic pricing
            consultation_fee = random.randint(150, 800)
            
            # Wait times
            wait_time = max(1, int(random.expovariate(1/14)))
            
            services.append({
                'id': f"mock_health_{area.lower()}_{i}_{int(time.time())}",
                'clinic': clinic,
                'specialty': specialty,
                'consultation_fee': consultation_fee,
                'wait_time_days': wait_time,
                'location': area,
                'rating': round(random.uniform(4.0, 5.0), 1),
                'accepts_insurance': random.choice([True, True, False]),
                'source': 'synthetic_demo',
                'is_synthetic': True,
                'scraped_at': datetime.now(timezone.utc).isoformat()
            })
        
        return services


# ============================================================
# INDUSTRY: HOSPITALITY
# ============================================================

class HospitalityMockData:
    """Generate synthetic hospitality venue data"""
    
    # ✅ SYNTHETIC VENUES (NO REAL NAMES)
    HOTELS = [
        ('Mock Hotel A', 0.15),
        ('Mock Hotel B', 0.14),
        ('Mock Hotel C', 0.13),
        ('Mock Hotel D', 0.12),
        ('Mock Hotel E', 0.11),
        ('Demo Resort A', 0.10),
        ('Demo Resort B', 0.09),
        ('Demo Resort C', 0.08),
        ('Demo Resort D', 0.08)
    ]
    
    RESTAURANTS = [
        ('Mock Restaurant A', 0.12),
        ('Mock Restaurant B', 0.11),
        ('Mock Restaurant C', 0.10),
        ('Mock Restaurant D', 0.09),
        ('Mock Restaurant E', 0.08),
        ('Demo Dining A', 0.08),
        ('Demo Dining B', 0.07),
        ('Demo Dining C', 0.06),
        ('Demo Dining D', 0.06)
    ]
    
    @staticmethod
    def generate_venues(area: str, count: int = 40) -> List[Dict]:
        """Generate synthetic hospitality venue data"""
        
        venues = []
        base_seed = int(hashlib.md5(f"hosp_{area}{datetime.now().date()}".encode()).hexdigest(), 16)
        random.seed(base_seed)
        
        for i in range(count):
            venue_type = random.choice(['hotel', 'restaurant'])
            
            if venue_type == 'hotel':
                name = random.choices(
                    [h[0] for h in HospitalityMockData.HOTELS],
                    weights=[h[1] for h in HospitalityMockData.HOTELS]
                )[0]
                avg_rate = random.randint(200, 1000)
                occupancy = round(random.uniform(0.65, 0.95), 2)
                
                venues.append({
                    'id': f"mock_hosp_{area.lower()}_{i}_{int(time.time())}",
                    'name': name,
                    'type': 'hotel',
                    'avg_room_rate': avg_rate,
                    'occupancy_rate': occupancy,
                    'location': area,
                    'rating': round(random.uniform(4.3, 5.0), 1),
                    'rooms': random.choice([150, 200, 250, 300]),
                    'source': 'synthetic_demo',
                    'is_synthetic': True,
                    'scraped_at': datetime.now(timezone.utc).isoformat()
                })
            else:
                name = random.choices(
                    [r[0] for r in HospitalityMockData.RESTAURANTS],
                    weights=[r[1] for r in HospitalityMockData.RESTAURANTS]
                )[0]
                avg_check = random.randint(50, 300)
                covers_per_day = random.randint(40, 120)
                
                venues.append({
                    'id': f"mock_hosp_{area.lower()}_{i}_{int(time.time())}",
                    'name': name,
                    'type': 'restaurant',
                    'avg_check': avg_check,
                    'covers_per_day': covers_per_day,
                    'location': area,
                    'rating': round(random.uniform(4.0, 5.0), 1),
                    'michelin_stars': random.choice([0, 1, 1, 2, 3]),
                    'source': 'synthetic_demo',
                    'is_synthetic': True,
                    'scraped_at': datetime.now(timezone.utc).isoformat()
                })
        
        return venues


# ============================================================
# UNIFIED MOCK DATA LOADER
# ============================================================

def load_mock_dataset(area: str, industry: str, max_items: int = 100) -> List[Dict]:
    """
    Load synthetic demo data for any industry
    
    Args:
        area: Geographic area/market
        industry: Industry code (real_estate, automotive, healthcare, hospitality)
        max_items: Maximum items to generate
    
    Returns:
        List of synthetic data items (all flagged with is_synthetic=True)
    """
    
    if industry == "real_estate":
        return RealEstateMockData.generate_properties(area, count=max_items)
    
    elif industry == "automotive":
        return AutomotiveMockData.generate_inventory(area, count=max_items)
    
    elif industry == "healthcare":
        return HealthcareMockData.generate_services(area, count=max_items)
    
    elif industry == "hospitality":
        return HospitalityMockData.generate_venues(area, count=max_items)
    
    else:
        # Default to real estate
        return RealEstateMockData.generate_properties(area, count=max_items)
