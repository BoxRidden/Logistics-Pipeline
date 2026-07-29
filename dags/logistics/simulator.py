import random
import logging
from uuid import uuid4
from datetime import datetime, timedelta
from logistics.profiles import CITIES, CATEGORIES, ORDER_TYPES

logger = logging.getLogger(__name__)

class ShipmentSimulator:
    def __init__(self, hubs: list, drivers: list):
        self.hubs = hubs
        self.drivers = drivers

    def generate_new_orders(self, num_records: int) -> list:
        """Creates completely new orders. They ALWAYS start as 'Pending'."""
        logger.info(f"Generating {num_records} brand new shipments...")
        shipments = []
        
        for _ in range(num_records):
            # Anomaly Injection (5% chance)
            if random.random() < 0.05:
                revenue = round(random.uniform(5000.0, 15000.0), 2)
                item_quantity = random.randint(100, 500)
            else:
                revenue = round(random.uniform(15.0, 150.0), 2)
                item_quantity = random.randint(1, 5)

            shipment = {
                "tracking_code": f"TRK-{str(uuid4())[:8].upper()}",
                "hub_id": random.choice(self.hubs) if self.hubs else 1,
                "driver_id": random.choice(self.drivers) if self.drivers else 1,
                "customer_city": random.choice(CITIES),
                "status": "Pending",  # NEW RULE: All new orders start here
                "revenue": revenue,
                "item_quantity": item_quantity,
                "product_category": random.choice(CATEGORIES),
                "order_type": random.choice(ORDER_TYPES)
            }
            shipments.append(shipment)
            
        return shipments

    def transition_existing_orders(self, active_orders_from_db: list) -> list:
        """
        Takes a list of existing dictionaries from the Postgres DB and 
        moves them through a logical state machine.
        """
        updated_shipments = []
        
        for order in active_orders_from_db:
            current_status = order.get('status')
            
            # State Machine Logic
            if current_status == 'Pending':
                # 80% chance to move to Transit, 20% to stay Pending
                order['status'] = 'In Transit' if random.random() < 0.8 else 'Pending'
                
            elif current_status == 'In Transit':
                # Delay Injection (10% chance)
                if random.random() < 0.10:
                    order['status'] = 'Delayed'
                else:
                    # 70% chance to be Delivered, 20% stays in Transit
                    order['status'] = 'Delivered' if random.random() < 0.7 else 'In Transit'
                    
            elif current_status == 'Delayed':
                # Eventually gets delivered
                order['status'] = 'Delivered' if random.random() < 0.5 else 'Delayed'

            # Append orders that actually changed status to send through CDC 
            if order['status'] != current_status:
                updated_shipments.append(order)

        logger.info(f"Successfully transitioned {len(updated_shipments)} existing orders.")
        return updated_shipments