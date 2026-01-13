"""
VOXMILL MULTI-INDUSTRY MOCK DATA GENERATOR
==========================================
Production-grade synthetic data for testing and demos

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
"""

import random
import time
from datetime import datetime, timezone, timedelta
from typing import Dict, List
import hashlib
import statistics

# ============================================================
# INDUSTRY: REAL ESTATE (LONDON LUXURY)
# ============================================================

class RealEstateMockData:
    """Generate realistic London luxury property data"""
    
    # Mayfair market parameters
    MAYFAIR_AGENTS = [
        ('Wetherell', 0.18),  # 18% market share
        ('Knight Frank Mayfair', 0.15),
        ('Savills Mayfair', 0.14),
        ('Strutt & Parker', 0.12),
        ('Chestertons Mayfair', 0.10),
        ('Beauchamp Estates', 0.09),
        ('Rokstone', 0.08),
        ('Aylesford International', 0.07),
        ('Aston Chase', 0.04),
        ('Private', 0.03)
    ]
    
    PROPERTY_TYPES = [
        ('Apartment', 0.45),
        ('Penthouse', 0.15),
        ('Townhouse', 0.20),
        ('Mews House', 0.12),
        ('Mansion', 0.08)
    ]
    
    MAYFAIR_STREETS = [
        'Park Lane', 'Grosvenor Square', 'Mount Street', 'South Audley Street',
        'Brook Street', 'Carlos Place', 'Green Street', 'North Audley Street',
        'Davies Street', 'Berkeley Square', 'Curzon Street', 'Charles Street'
    ]
    
    @staticmethod
    def generate_properties(area: str, count: int = 75) -> List[Dict]:
        """Generate realistic property listings"""
        
        properties = []
        base_seed = int(hashlib.md5(f"{area}{datetime.now().date()}".encode()).hexdigest(), 16)
        random.seed(base_seed)
        
        # Market parameters by area
        market_params = {
            'Mayfair': {'avg_price': 3500000, 'std_dev': 1200000, 'avg_sqft': 2200},
            'Knightsbridge': {'avg_price': 3200000, 'std_dev': 1100000, 'avg_sqft': 2000},
            'Chelsea': {'avg_price': 2800000, 'std_dev': 950000, 'avg_sqft': 1900},
            'Belgravia': {'avg_price': 3800000, 'std_dev': 1400000, 'avg_sqft': 2400},
            'Kensington': {'avg_price': 2500000, 'std_dev': 800000, 'avg_sqft': 1800}
        }
        
        params = market_params.get(area, market_params['Mayfair'])
        
        for i in range(count):
            # Select agent based on market share
            agent = random.choices(
                [a[0] for a in RealEstateMockData.MAYFAIR_AGENTS],
                weights=[a[1] for a in RealEstateMockData.MAYFAIR_AGENTS]
            )[0]
            
            # Select property type
            prop_type = random.choices(
                [t[0] for t in RealEstateMockData.PROPERTY_TYPES],
                weights=[t[1] for t in RealEstateMockData.PROPERTY_TYPES]
            )[0]
            
            # Generate realistic price (normal distribution)
            price = max(500000, int(random.gauss(params['avg_price'], params['std_dev'])))
            
            # Round to nearest £50k
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
            
            # Days on market (realistic distribution)
            days_on_market = max(1, int(random.expovariate(1/45)))
            
            # Address
            street = random.choice(RealEstateMockData.MAYFAIR_STREETS)
            number = random.randint(1, 150)
            address = f"{number} {street}, {area}, London"
            
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
                'source': 'mock_data',
                'scraped_at': datetime.now(timezone.utc).isoformat()
            })
        
        return properties


# ============================================================
# INDUSTRY: AUTOMOTIVE (PRESTIGE DEALERSHIPS)
# ============================================================

class AutomotiveMockData:
    """Generate realistic prestige car dealership data"""
    
    DEALERS = [
        ('HR Owen Mayfair', 0.22),
        ('Jack Barclay Bentley', 0.18),
        ('H.R. Owen Ferrari', 0.15),
        ('Romans International', 0.12),
        ('Stratstone', 0.10),
        ('JCT600', 0.08),
        ('Carrs Ferrari', 0.07),
        ('Dick Lovett', 0.05),
        ('Private Sale', 0.03)
    ]
    
    BRANDS = [
        ('Bentley', 0.20), ('Ferrari', 0.18), ('Rolls-Royce', 0.15),
        ('Lamborghini', 0.12), ('McLaren', 0.10), ('Aston Martin', 0.10),
        ('Porsche', 0.08), ('Maserati', 0.05), ('Bugatti', 0.02)
    ]
    
    MODELS = {
        'Bentley': ['Continental GT', 'Flying Spur', 'Bentayga'],
        'Ferrari': ['812 Superfast', 'F8 Tributo', 'SF90 Stradale', 'Roma'],
        'Rolls-Royce': ['Phantom', 'Ghost', 'Cullinan', 'Wraith'],
        'Lamborghini': ['Aventador', 'Huracan', 'Urus'],
        'McLaren': ['720S', 'GT', 'Artura'],
        'Aston Martin': ['DB12', 'Vantage', 'DBX'],
        'Porsche': ['911 Turbo S', 'Taycan', 'Cayenne Turbo'],
        'Maserati': ['MC20', 'Quattroporte', 'Levante'],
        'Bugatti': ['Chiron', 'Divo']
    }
    
    @staticmethod
    def generate_inventory(area: str, count: int = 60) -> List[Dict]:
        """Generate realistic prestige car inventory"""
        
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
            
            # Generate price based on brand
            brand_prices = {
                'Bentley': (200000, 350000),
                'Ferrari': (250000, 500000),
                'Rolls-Royce': (300000, 550000),
                'Lamborghini': (200000, 400000),
                'McLaren': (180000, 350000),
                'Aston Martin': (150000, 300000),
                'Porsche': (120000, 250000),
                'Maserati': (100000, 200000),
                'Bugatti': (2500000, 3500000)
            }
            
            price_range = brand_prices[brand]
            price = random.randint(price_range[0], price_range[1])
            price = round(price / 5000) * 5000  # Round to nearest £5k
            
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
                'source': 'mock_data',
                'scraped_at': datetime.now(timezone.utc).isoformat()
            })
        
        return inventory


# ============================================================
# INDUSTRY: HEALTHCARE (PRIVATE CLINICS)
# ============================================================

class HealthcareMockData:
    """Generate realistic private healthcare clinic data"""
    
    CLINICS = [
        ('The London Clinic', 0.20),
        ('The Wellington Hospital', 0.18),
        ('The Princess Grace Hospital', 0.15),
        ('The Lister Hospital', 0.12),
        ('The Harley Street Clinic', 0.10),
        ('BMI The London Independent', 0.08),
        ('The Cromwell Hospital', 0.08),
        ('King Edward VII Hospital', 0.06),
        ('Private Practice', 0.03)
    ]
    
    SPECIALTIES = [
        ('Cardiology', 0.18), ('Orthopedics', 0.16), ('Oncology', 0.14),
        ('Neurology', 0.12), ('Dermatology', 0.10), ('Ophthalmology', 0.10),
        ('Plastic Surgery', 0.08), ('ENT', 0.07), ('Gastroenterology', 0.05)
    ]
    
    @staticmethod
    def generate_services(area: str, count: int = 50) -> List[Dict]:
        """Generate realistic healthcare service data"""
        
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
            
            # Pricing by specialty
            specialty_prices = {
                'Cardiology': (250, 600),
                'Orthopedics': (200, 500),
                'Oncology': (300, 800),
                'Neurology': (250, 650),
                'Dermatology': (150, 400),
                'Ophthalmology': (180, 450),
                'Plastic Surgery': (500, 2000),
                'ENT': (150, 400),
                'Gastroenterology': (200, 500)
            }
            
            price_range = specialty_prices[specialty]
            consultation_fee = random.randint(price_range[0], price_range[1])
            
            # Wait times (days)
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
                'source': 'mock_data',
                'scraped_at': datetime.now(timezone.utc).isoformat()
            })
        
        return services


# ============================================================
# INDUSTRY: HOSPITALITY (PREMIUM HOTELS/RESTAURANTS)
# ============================================================

class HospitalityMockData:
    """Generate realistic premium hospitality data"""
    
    HOTELS = [
        ('The Ritz London', 0.15),
        ('Claridge\'s', 0.14),
        ('The Savoy', 0.13),
        ('The Dorchester', 0.12),
        ('The Connaught', 0.11),
        ('The Berkeley', 0.10),
        ('Mandarin Oriental', 0.09),
        ('The Langham', 0.08),
        ('Four Seasons Park Lane', 0.08)
    ]
    
    RESTAURANTS = [
        ('Restaurant Gordon Ramsay', 0.12),
        ('Alain Ducasse at The Dorchester', 0.11),
        ('The Ledbury', 0.10),
        ('Core by Clare Smyth', 0.09),
        ('Sketch', 0.08),
        ('Hélène Darroze', 0.08),
        ('Le Gavroche', 0.07),
        ('The Clove Club', 0.06),
        ('Dinner by Heston', 0.06)
    ]
    
    @staticmethod
    def generate_venues(area: str, count: int = 40) -> List[Dict]:
        """Generate realistic hospitality venue data"""
        
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
                avg_rate = random.randint(400, 1500)
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
                    'source': 'mock_data',
                    'scraped_at': datetime.now(timezone.utc).isoformat()
                })
            else:
                name = random.choices(
                    [r[0] for r in HospitalityMockData.RESTAURANTS],
                    weights=[r[1] for r in HospitalityMockData.RESTAURANTS]
                )[0]
                avg_check = random.randint(80, 350)
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
                    'source': 'mock_data',
                    'scraped_at': datetime.now(timezone.utc).isoformat()
                })
        
        return venues


# ============================================================
# UNIFIED MOCK DATA LOADER
# ============================================================

def load_mock_dataset(area: str, industry: str, max_items: int = 100) -> List[Dict]:
    """
    Load mock data for any industry
    
    Args:
        area: Geographic area/market
        industry: Industry code (real_estate, automotive, healthcare, hospitality)
        max_items: Maximum items to generate
    
    Returns:
        List of mock data items
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
