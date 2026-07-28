"""
Logistics Master Data and Weather Profiles

This module contains static master data for the shipment simulator
and the business logic mapping weather conditions to supply chain impacts.
"""

#Master Data Constants 
CITIES = ['Hanoi', 'Da Nang', 'Ho Chi Minh City']

#Added 'Delayed' as valid status fallback 
STATUSES = ['Pending', 'In Transit', 'Delayed', 'Delivered', 'Cancelled'] 

CATEGORIES = ['Electronics', 'Clothing', 'Groceries', 'Furniture', 'Documents']
ORDER_TYPES = ['Standard', 'Express', 'Next-Day']

HUBS = [
    (1, 'Hanoi Central', 'Hanoi', 21.0285, 105.8542),
    (2, 'Da Nang Hub', 'Da Nang', 16.0471, 108.2068),
    (3, 'HCM City Base', 'Ho Chi Minh City', 10.8231, 106.6297)
]

DRIVERS = [
    (1, 'Nguyen Van A', 'Truck'),
    (2, 'Tran Thi B', 'Motorcycle'),
    (3, 'Le Bron C', 'Van') 
]


# Weather Impact Profiles 
WEATHER_LOGISTICS_PROFILES: dict[str, dict] = {
    # Severe Disruptions 
    "Thunderstorm":  {"delay_prob": 0.85, "cancel_prob": 0.10, "transit_time_multiplier": 2.5, "status_weights": {"In Transit": 2, "Delayed": 7, "Delivered": 1}},
    "Heavy snow":    {"delay_prob": 0.90, "cancel_prob": 0.15, "transit_time_multiplier": 3.0, "status_weights": {"In Transit": 1, "Delayed": 8, "Delivered": 1}},
    "Tornado":       {"delay_prob": 0.99, "cancel_prob": 0.50, "transit_time_multiplier": 5.0, "status_weights": {"In Transit": 1, "Delayed": 9, "Delivered": 0}},
    "Freezing rain": {"delay_prob": 0.80, "cancel_prob": 0.05, "transit_time_multiplier": 2.0, "status_weights": {"In Transit": 3, "Delayed": 6, "Delivered": 1}},
    "Dusty":         {"delay_prob": 0.65, "cancel_prob": 0.05, "transit_time_multiplier": 1.6, "status_weights": {"In Transit": 4, "Delayed": 5, "Delivered": 1}},

    # Moderate Disruptions 
    "Rain":          {"delay_prob": 0.40, "cancel_prob": 0.01, "transit_time_multiplier": 1.3, "status_weights": {"In Transit": 5, "Delayed": 3, "Delivered": 2}},
    "Snow":          {"delay_prob": 0.60, "cancel_prob": 0.03, "transit_time_multiplier": 1.7, "status_weights": {"In Transit": 4, "Delayed": 5, "Delivered": 1}},
    "Fog":           {"delay_prob": 0.55, "cancel_prob": 0.02, "transit_time_multiplier": 1.5, "status_weights": {"In Transit": 4, "Delayed": 4, "Delivered": 2}},
    "Drizzle":       {"delay_prob": 0.20, "cancel_prob": 0.00, "transit_time_multiplier": 1.1, "status_weights": {"In Transit": 6, "Delayed": 1, "Delivered": 3}},
    "Windy":         {"delay_prob": 0.30, "cancel_prob": 0.01, "transit_time_multiplier": 1.2, "status_weights": {"In Transit": 5, "Delayed": 2, "Delivered": 3}},

    # Optimal Conditions 
    "Clear":         {"delay_prob": 0.05, "cancel_prob": 0.00, "transit_time_multiplier": 1.0, "status_weights": {"In Transit": 4, "Delayed": 0, "Delivered": 6}},
    "Clouds":        {"delay_prob": 0.08, "cancel_prob": 0.00, "transit_time_multiplier": 1.0, "status_weights": {"In Transit": 5, "Delayed": 0, "Delivered": 5}},
    "Overcast":      {"delay_prob": 0.10, "cancel_prob": 0.00, "transit_time_multiplier": 1.0, "status_weights": {"In Transit": 5, "Delayed": 1, "Delivered": 4}},
    "Sunny":         {"delay_prob": 0.02, "cancel_prob": 0.00, "transit_time_multiplier": 1.0, "status_weights": {"In Transit": 3, "Delayed": 0, "Delivered": 7}},
}

# Fallback profile for unknown API conditions to prevent simulator crashes
DEFAULT_PROFILE: dict = {
    "delay_prob": 0.15,
    "cancel_prob": 0.00,
    "transit_time_multiplier": 1.1,
    "status_weights": {"In Transit": 5, "Delayed": 1, "Delivered": 4},
}

# Lookup index (case-insensitive) to handle mismatches from OpenMeteo/Tomorrow.io
_PROFILES_LOWER: dict[str, dict] = {k.lower(): v for k, v in WEATHER_LOGISTICS_PROFILES.items()}

def get_logistics_profile(condition: str) -> dict:
    """
    Retrieves the logistics impact profile based on a weather condition string.
    Returns a safe DEFAULT_PROFILE if the condition is not mapped.
    """
    if not condition:
        return DEFAULT_PROFILE
        
    profile = _PROFILES_LOWER.get(condition.lower())
    if profile is None:
        print(f"[WARN] profiles.py: Unknown weather condition '{condition}' -> falling back to DEFAULT")
    
    return profile or DEFAULT_PROFILE
