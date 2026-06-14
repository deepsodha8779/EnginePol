# Content Hash

The content hash is a deterministic SHA-256 digest over the canonicalized event body.

Rules:
- Only the `body` is hashed. Head/tail fields (tenant id, event id, timestamps) are excluded.
- Object keys are sorted to ensure stable ordering.
- Array order is preserved.
- Output format is `sha256:<lowercase hex>`.

Example:

```json
{
  "head": {
    "content_hash": "sha256:9894573cc8ba673ff3813df2836b194a3b749ba3b198680fc4487090789cfe2f"
  }
}
```
