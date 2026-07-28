import random
import logging
from uuid import uuid4
from logistics.profiles import CITIES, CATEGORIES, ORDER_TYPES

# Set up the named logger
logger = logging.getLogger(__name__)

class ShipmentSimulator:
    def __init__(self, hubs: list, drivers: list):
        """
        Initialize with active hubs and drivers from the database 
        to prevent foreign key constraint violations.
        """
        self.hubs = hubs
        self.drivers = drivers

    def generate_payload(self, num_records: int) -> list:
        logger.info(f"Generating {num_records} new simulated shipments...")
        shipments = []
        
        for _ in range(num_records):
            shipment = {
                # UUIDs for public-facing tracking numbers
                "tracking_code": f"TRK-{str(uuid4())[:8].upper()}",
                
                # Foreign keys
                "hub_id": random.choice(self.hubs) if self.hubs else 1,
                "driver_id": random.choice(self.drivers) if self.drivers else 1,
                
                "customer_city": random.choice(CITIES),
                
                # All new operational data starts as Pending.
                "status": 'Pending',
                
                "revenue": round(random.uniform(15.0, 150.0), 2),
                "item_quantity": random.randint(1, 5),
                "product_category": random.choice(CATEGORIES),
                "order_type": random.choice(ORDER_TYPES)
            }
            shipments.append(shipment)
            
        logger.info(f"Successfully generated payload of {len(shipments)} records.")
        return shipments