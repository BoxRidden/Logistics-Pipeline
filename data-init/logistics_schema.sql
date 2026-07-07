-- Drop tables if resetting environment during development
DROP TABLE IF EXISTS shipments CASCADE;
DROP TABLE IF EXISTS drivers CASCADE;
DROP TABLE IF EXISTS hubs CASCADE;

CREATE TABLE hubs (
    hub_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    city VARCHAR(100) NOT NULL,
    lat DECIMAL(9,6) NOT NULL,
    lon DECIMAL(9,6) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE drivers (
    driver_id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    vehicle_type VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE shipments (
    shipment_id SERIAL PRIMARY KEY,
    tracking_code VARCHAR(50) UNIQUE NOT NULL,
    hub_id INT REFERENCES hubs(hub_id),
    driver_id INT REFERENCES drivers(driver_id),
    customer_city VARCHAR(100) NOT NULL,
    status VARCHAR(50) NOT NULL CHECK (status IN ('Pending', 'In Transit', 'Delivered', 'Delayed', 'Cancelled')),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Performance Indexes for Extraction and Joins
CREATE INDEX shipments_updated_at ON shipments(updated_at);
CREATE INDEX shipments_hub_status ON shipments(hub_id, status);