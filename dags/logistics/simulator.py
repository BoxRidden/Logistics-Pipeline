import random
from logistics.profiles import CITIES, CATEGORIES, ORDER_TYPES

class ShipmentSimulator:
    def generate_payload(self, num_records):
        shipments = []
        
        status_options = ['Pending', 'In Transit', 'Delivered', 'Delayed', 'Cancelled']
        status_weights = [10, 30, 45, 10, 5]
        
        for i in range(num_records):
            # Guarantee at least one of each status exists in the payload by overriding the randomizer for the first 5 iterations
            if i < len(status_options):
                assigned_status = status_options[i]
            else:
                assigned_status = random.choices(status_options, weights=status_weights, k=1)[0]

            shipment = {
                "tracking_code": f"TRK-{random.randint(10000, 99999)}",
                "hub_id": random.randint(1, 3),
                "driver_id": random.randint(1, 3),
                "customer_city": random.choice(CITIES),
                "status": assigned_status,
                "revenue": round(random.uniform(15.0, 150.0), 2),
                "item_quantity": random.randint(1, 5),
                "product_category": random.choice(CATEGORIES),
                "order_type": random.choice(ORDER_TYPES)
            }
            shipments.append(shipment)
            
        return shipments