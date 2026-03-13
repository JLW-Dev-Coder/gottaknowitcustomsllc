CREATE TABLE IF NOT EXISTS operators (
  profile_slug TEXT PRIMARY KEY,
  profile_type TEXT NOT NULL DEFAULT 'operator',
  operator_name TEXT,
  business_name TEXT,
  business_phone TEXT,
  support_email TEXT,
  business_address TEXT,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_operators_business_name
ON operators (business_name);

CREATE INDEX IF NOT EXISTS idx_operators_operator_name
ON operators (operator_name);

CREATE TABLE IF NOT EXISTS audit_log (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  action TEXT NOT NULL,
  actor TEXT NOT NULL,
  profile_slug TEXT NOT NULL,
  payload_json TEXT,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_audit_profile_slug
ON audit_log (profile_slug);

CREATE INDEX IF NOT EXISTS idx_audit_created_at
ON audit_log (created_at);
