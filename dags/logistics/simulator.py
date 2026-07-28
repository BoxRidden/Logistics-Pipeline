import random
import logging
from uuid import uuid4
from logistics.profiles import CITIES, CATEGORIES, ORDER_TYPES

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
            
            # 1. Anomaly Injection
            # 5% chance to generate a bizarre order
            if random.random() < 0.05:
                revenue = round(random.uniform(5000.0, 15000.0), 2)  # Massive revenue
                item_quantity = random.randint(100, 500)             # Massive quantity
            else:
                # Normal operational data
                revenue = round(random.uniform(15.0, 150.0), 2)
                item_quantity = random.randint(1, 5)

            # 2. Delay Injection
            # 10% chance of delay, otherwise standard operational statuses
            status_list = ['Pending', 'In Transit', 'Delivered', 'Delayed']
            status_weights = [0.40, 0.30, 0.20, 0.10]
            simulated_status = random.choices(status_list, weights=status_weights, k=1)[0]

            shipment = {
                "tracking_code": f"TRK-{str(uuid4())[:8].upper()}",
                "hub_id": random.choice(self.hubs) if self.hubs else 1,
                "driver_id": random.choice(self.drivers) if self.drivers else 1,
                "customer_city": random.choice(CITIES),
                "status": simulated_status,  
                "revenue": revenue,
                "item_quantity": item_quantity,
                "product_category": random.choice(CATEGORIES),
                "order_type": random.choice(ORDER_TYPES)
            }
            shipments.append(shipment)
            
        logger.info(f"Successfully generated payload of {len(shipments)} records.")
        return shipments