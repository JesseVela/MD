# Upload folder (S3-style)

- **Canonical path:** `data/upload/<client_id>/<file_name>`
- **Sample client:** Use `acme` (aligned with Kristaq/Bhavin). Put CSVs in `data/upload/acme/`.
- **Example:** `data/upload/acme/sample_upload.csv`
- A legacy folder `client_acme/` may exist from older runs; the pipeline uses `acme/` only. You can delete `client_acme/` to avoid confusion.
