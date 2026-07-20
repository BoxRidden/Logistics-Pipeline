import psycopg2

class PostgresRepository:
    def __init__(self, host, database, user, password):
        self.conn = psycopg2.connect(host=host, database=database, user=user, password=password)
        self.cursor = self.conn.cursor()

    def initialize_schema(self, hubs, drivers):
        # Removed DROP TABLE statements so history is preserved
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS hubs (
            hub_id INT PRIMARY KEY, name VARCHAR(50), city VARCHAR(50), lat FLOAT, lon FLOAT,
            valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP, valid_to TIMESTAMP DEFAULT NULL, is_current BOOLEAN DEFAULT TRUE
        );

        CREATE TABLE IF NOT EXISTS drivers (
            driver_id INT PRIMARY KEY, name VARCHAR(50), vehicle_type VARCHAR(50),
            valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP, valid_to TIMESTAMP DEFAULT NULL, is_current BOOLEAN DEFAULT TRUE
        );

        CREATE TABLE IF NOT EXISTS shipments (
            shipment_id SERIAL PRIMARY KEY, tracking_code VARCHAR(50), hub_id INT,
            driver_id INT, customer_city VARCHAR(50), status VARCHAR(20),
            revenue FLOAT, item_quantity INT, product_category VARCHAR(50), order_type VARCHAR(20),
            created_at TIMESTAMP, updated_at TIMESTAMP
        );
        """)

        # Added ON CONFLICT to safely ignore dimensions if they already exist
        for h in hubs:
            self.cursor.execute(f"INSERT INTO hubs (hub_id, name, city, lat, lon) VALUES ({h[0]}, '{h[1]}', '{h[2]}', {h[3]}, {h[4]}) ON CONFLICT (hub_id) DO NOTHING;")

        for d in drivers:
            self.cursor.execute(f"INSERT INTO drivers (driver_id, name, vehicle_type) VALUES ({d[0]}, '{d[1]}', '{d[2]}') ON CONFLICT (driver_id) DO NOTHING;")

        self.conn.commit()

    def insert_shipments(self, shipments_list):
        for s in shipments_list:
            self.cursor.execute(f"""
            INSERT INTO shipments (tracking_code, hub_id, driver_id, customer_city, status, revenue, item_quantity, product_category, order_type, created_at, updated_at)
            VALUES ('{s['tracking_code']}', {s['hub_id']}, {s['driver_id']}, '{s['customer_city']}', '{s['status']}', {s['revenue']}, {s['item_quantity']}, '{s['product_category']}', '{s['order_type']}', NOW(), NOW());
            """)
        self.conn.commit()

    def close(self):
        self.cursor.close()
        self.conn.close()