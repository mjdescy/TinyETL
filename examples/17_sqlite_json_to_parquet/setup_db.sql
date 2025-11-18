-- Create table with JSON column
CREATE TABLE names (
  id TEXT,
  url TEXT,
  name TEXT,
  doc JSON
);

-- Insert sample data with JSON documents
INSERT INTO names (id, url, name, doc) VALUES
  ('1', 'https://example.com/user1', 'Alice Johnson', '{"age": 30, "city": "New York", "active": true, "tags": ["developer", "manager"]}'),
  ('2', 'https://example.com/user2', 'Bob Smith', '{"age": 25, "city": "San Francisco", "active": true, "tags": ["designer", "frontend"]}'),
  ('3', 'https://example.com/user3', 'Carol Davis', '{"age": 35, "city": "Boston", "active": false, "tags": ["backend", "architect"]}'),
  ('4', 'https://example.com/user4', 'David Wilson', '{"age": 28, "city": "Seattle", "active": true, "tags": ["devops", "security"]}'),
  ('5', 'https://example.com/user5', 'Eve Martinez', '{"age": 32, "city": "Austin", "active": true, "tags": ["data", "analytics"]}');
