# ADR-002: Clustering strategy
**Decision:** Prefer server-side clustering for large/busy views; keep client-side when result sets are small.
**Why:** Smaller payloads and faster first paint at scale.
