-- Seed stock idempotently: re-runs on every startup, existing rows untouched.
INSERT INTO stock (sku, quantity) VALUES
    ('WIDGET-1', 50),
    ('WIDGET-2', 5),
    ('WIDGET-3', 0)
ON CONFLICT (sku) DO NOTHING;
