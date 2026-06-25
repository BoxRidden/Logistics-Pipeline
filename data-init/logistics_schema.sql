-- data-init/01_logistics_schema.sql

CREATE TABLE IF NOT EXISTS hubs (
    hub_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    city VARCHAR(100),
    lat DECIMAL(9,6),
    lon DECIMAL(9,6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS drivers (
    driver_id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    vehicle_type VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS shipments (
    shipment_id SERIAL PRIMARY KEY,
    tracking_code VARCHAR(50) UNIQUE,
    hub_id INT REFERENCES hubs(hub_id),
    driver_id INT REFERENCES drivers(driver_id),
    customer_city VARCHAR(100),
    status VARCHAR(50), -- 'Pending', 'In Transit', 'Delivered', 'Delayed'
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert base Hubs (Cities we will track weather for)
INSERT INTO hubs (name, city, lat, lon) VALUES
('Hanoi Central Hub', 'Hanoi', 21.0285, 105.8542),
('Da Nang Port Hub', 'Da Nang', 16.0471, 108.2068),
('HCM South Hub', 'Ho Chi Minh City', 10.8231, 106.6297)
ON CONFLICT DO NOTHING;

-- Insert base Drivers
INSERT INTO drivers (name, vehicle_type) VALUES
('Nguyen Van A', 'Motorcycle'),
('Tran Thi B', 'Truck'),
('Le Van C', 'Van')
ON CONFLICT DO NOTHING;