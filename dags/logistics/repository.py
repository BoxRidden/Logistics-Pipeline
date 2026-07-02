import psycopg2

class PostgresRepository:
    def __init__(self, host, database, user, password):
        self.conn = psycopg2.connect(host=host, database=database, user=user, password=password)
        self.cursor = self.conn.cursor()

    def initialize_schema(self, hubs, drivers):
        # 1. Create the tables with all the new Looker columns
        self.cursor.execute("""
        CREATE TABLE IF NOT EXISTS hubs (
            hub_id SERIAL PRIMARY KEY, name VARCHAR(50), city VARCHAR(50), lat FLOAT, lon FLOAT
        );
        CREATE TABLE IF NOT EXISTS drivers (
            driver_id SERIAL PRIMARY KEY, name VARCHAR(50), vehicle_type VARCHAR(50), created_at TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS shipments (
            shipment_id SERIAL PRIMARY KEY, tracking_code VARCHAR(50), hub_id INT,
            driver_id INT, customer_city VARCHAR(50), status VARCHAR(20),
            revenue FLOAT, item_quantity INT, product_category VARCHAR(50), order_type VARCHAR(20),
            created_at TIMESTAMP, updated_at TIMESTAMP
        );
        """)

        # 2. Seed Hubs if the table is empty
        self.cursor.execute("SELECT COUNT(*) FROM hubs;")
        if self.cursor.fetchone()[0] == 0:
            for h in hubs:
                self.cursor.execute(f"INSERT INTO hubs (hub_id, name, city, lat, lon) VALUES ({h[0]}, '{h[1]}', '{h[2]}', {h[3]}, {h[4]});")

        # 3. Seed Drivers if the table is empty
        self.cursor.execute("SELECT COUNT(*) FROM drivers;")
        if self.cursor.fetchone()[0] == 0:
            for d in drivers:
                self.cursor.execute(f"INSERT INTO drivers (driver_id, name, vehicle_type, created_at) VALUES ({d[0]}, '{d[1]}', '{d[2]}', NOW());")

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