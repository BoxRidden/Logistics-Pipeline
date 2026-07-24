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

        # Using %s instead of f-strings to prevent SQL injection
        for h in hubs:
            self.cursor.execute(
                "INSERT INTO hubs (hub_id, name, city, lat, lon) VALUES (%s, %s, %s, %s, %s) ON CONFLICT (hub_id) DO NOTHING;",
                (h[0], h[1], h[2], h[3], h[4])
            )

        for d in drivers:
            self.cursor.execute(
                "INSERT INTO drivers (driver_id, name, vehicle_type) VALUES (%s, %s, %s) ON CONFLICT (driver_id) DO NOTHING;",
                (d[0], d[1], d[2])
            )

        self.conn.commit()

    def insert_shipments(self, shipments_list):
        for s in shipments_list:
            # Hardcoded 'Pending' status, swapped f-strings for %s
            self.cursor.execute("""
            INSERT INTO shipments (
                tracking_code, hub_id, driver_id, customer_city, status, 
                revenue, item_quantity, product_category, order_type, created_at, updated_at
            )
            VALUES (%s, %s, %s, %s, 'Pending', %s, %s, %s, %s, NOW(), NOW()); 
            """, (
                s['tracking_code'], s['hub_id'], s['driver_id'], s['customer_city'], 
                s['revenue'], s['item_quantity'], s['product_category'], s['order_type']
            ))
        self.conn.commit()

    def advance_shipment_status(self) -> tuple[int, int, int]:
        """
        Every UPDATE changes the 'updated_at' timestamp, forcing downstream CDC to capture it.
        """
        # Pending -> In Transit (60% chance, or forced if older than 2 hours)
        self.cursor.execute("""
            UPDATE shipments
            SET status = 'In Transit', updated_at = NOW()
            WHERE status = 'Pending'
              AND (RANDOM() < 0.6 OR EXTRACT(EPOCH FROM (NOW() - updated_at))/60 > 120)
        """)
        n_transit = self.cursor.rowcount

        # In Transit -> Delivered (60% chance, strictly older than 1 hour)
        self.cursor.execute("""
            UPDATE shipments
            SET status = 'Delivered', updated_at = NOW()
            WHERE status = 'In Transit'
              AND EXTRACT(EPOCH FROM (NOW() - updated_at))/60 > 60
              AND (RANDOM() < 0.6 OR EXTRACT(EPOCH FROM (NOW() - updated_at))/60 > 120)
        """)
        n_delivered = self.cursor.rowcount

        # Pending -> Cancelled (Random 5% of all Pending orders)
        self.cursor.execute("""
            UPDATE shipments
            SET status = 'Cancelled', updated_at = NOW()
            WHERE shipment_id IN (
                SELECT shipment_id FROM shipments
                WHERE status = 'Pending'
                ORDER BY RANDOM()
                LIMIT (SELECT GREATEST(1, COUNT(*) / 20) FROM shipments WHERE status = 'Pending')
            )
        """)
        n_cancelled = self.cursor.rowcount
        
        self.conn.commit()
        return n_transit, n_delivered, n_cancelled

    def close(self):
        self.cursor.close()
        self.conn.close()