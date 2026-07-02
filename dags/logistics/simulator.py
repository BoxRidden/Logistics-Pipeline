import random
from logistics.profiles import CITIES, STATUSES, CATEGORIES, ORDER_TYPES

class ShipmentSimulator:
    def generate_payload(self, num_records):
        shipments = []
        for _ in range(num_records):
            shipment = {
                "tracking_code": f"TRK-{random.randint(10000, 99999)}",
                "hub_id": random.randint(1, 3),
                "driver_id": random.randint(1, 3),
                "customer_city": random.choice(CITIES),
                "status": random.choice(STATUSES),
                "revenue": round(random.uniform(15.0, 150.0), 2),
                "item_quantity": random.randint(1, 5),
                "product_category": random.choice(CATEGORIES),
                "order_type": random.choice(ORDER_TYPES)
            }
            shipments.append(shipment)
        return shipments