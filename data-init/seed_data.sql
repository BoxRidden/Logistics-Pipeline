-- ============================================================================
-- FILE: seed_data.sql
-- DESCRIPTION: Populates initial dimensional baseline data (Hubs, Drivers).
-- REVIEWER NOTE: Uses ON CONFLICT DO NOTHING to ensure idempotency during
-- automated integration testing or container re-initialization.
-- ============================================================================

-- Seed Hubs (Dimensional Baseline for Weather Joins)
INSERT INTO hubs (name, city, lat, lon) VALUES
('Hanoi Central Hub', 'Hanoi', 21.028500, 105.854200),
('Da Nang Port Hub', 'Da Nang', 16.047100, 108.206800),
('HCM South Hub', 'Ho Chi Minh City', 10.823100, 106.629700)
ON CONFLICT (name) DO NOTHING;

-- Seed Drivers (Dimensional Baseline for Delivery Assignments)
INSERT INTO drivers (name, vehicle_type) VALUES
('Nguyen Van A', 'Motorcycle'),
('Tran Thi B', 'Truck'),
('Le Van C', 'Van')
ON CONFLICT DO NOTHING; 