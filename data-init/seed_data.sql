-- ============================================================================
-- FILE: seed_data.sql
-- DESCRIPTION: Populates initial dimensional baseline data (Hubs, Drivers).
-- REVIEWER NOTE: Uses ON CONFLICT DO NOTHING to ensure idempotency during
-- automated integration testing or container re-initialization.
-- ============================================================================

-- Seed Hubs (Dimensional Baseline for Weather Joins)
-- Explicitly seeding SCD Type 2 initial states
INSERT INTO hubs (hub_id, name, city, lat, lon, valid_from, is_current) VALUES
(1, 'Hanoi Central Hub', 'Hanoi', 21.028500, 105.854200, '2026-01-01 00:00:00', TRUE),
(2, 'Da Nang Port Hub', 'Da Nang', 16.047100, 108.206800, '2026-01-01 00:00:00', TRUE),
(3, 'HCM South Hub', 'Ho Chi Minh City', 10.823100, 106.629700, '2026-01-01 00:00:00', TRUE)
ON CONFLICT DO NOTHING;

INSERT INTO drivers (driver_id, name, vehicle_type, valid_from, is_current) VALUES
(1, 'Nguyen Van A', 'Motorcycle', '2026-01-01 00:00:00', TRUE),
(2, 'Tran Thi B', 'Truck', '2026-01-01 00:00:00', TRUE),
(3, 'Le Van C', 'Van', '2026-01-01 00:00:00', TRUE)
ON CONFLICT DO NOTHING;