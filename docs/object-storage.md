# Object Storage Configuration

Joxette delegates all object storage I/O to DuckDB's `httpfs` extension via
[DuckLake](https://ducklake.select/). There is no Java object-storage SDK in
the dependency tree — every Parquet file write and read goes through a
DuckDB `CREATE SECRET` that Joxette registers at startup.

The only mandatory setting is the **data path** that DuckLake uses as the
root for Parquet files:

```yaml
joxette:
  catalog:
    object-storage-path: "s3://my-bucket/joxette/data"   # provider-specific, see below
```

---

## Amazon S3

### Path format

```
s3://<bucket>/<prefix>
```

Example:

```
s3://acme-joxette/prod/data
```

### DuckDB secret (what Joxette generates internally)

Joxette calls `CREATE SECRET` on startup when `joxette.s3.endpoint` is set
(MinIO / LocalStack). For real AWS the credential chain is used and no
explicit secret is needed (see [credential chain](#aws-credential-chain)).

```sql
-- Generated automatically when joxette.s3.endpoint is set
CREATE OR REPLACE SECRET joxette_s3 (
    TYPE        S3,
    KEY_ID      '<access-key-id>',
    SECRET      '<secret-access-key>',
    REGION      'us-east-1',
    ENDPOINT    'localhost:9000',   -- host:port, no scheme
    USE_SSL     false,
    URL_STYLE   'path'             -- required for MinIO and LocalStack
);
```

For standard AWS leave `endpoint` blank; DuckDB resolves credentials via
the default provider chain (instance profile, `~/.aws/credentials`, environment
variables `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`).

### IAM permissions

The IAM principal (user or role) running Joxette needs:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "JoxetteParquet",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:DeleteObject",
        "s3:ListBucket",
        "s3:GetBucketLocation"
      ],
      "Resource": [
        "arn:aws:s3:::acme-joxette",
        "arn:aws:s3:::acme-joxette/*"
      ]
    }
  ]
}
```

`s3:DeleteObject` is required for compaction (DuckLake marks old Parquet
files for deletion after merges).

### AWS credential chain

When `joxette.s3.endpoint` is blank, Joxette skips the explicit
`CREATE SECRET` call and lets DuckDB resolve credentials itself in this
order:

1. Environment variables — `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`,
   `AWS_SESSION_TOKEN`, `AWS_DEFAULT_REGION`
2. `~/.aws/credentials` / `~/.aws/config`
3. EC2 instance metadata service (IMDSv2)
4. ECS / EKS task role

For EKS with IRSA, no static credentials are needed — annotate the service
account and set only the region.

### application.yml — AWS production

```yaml
joxette:
  catalog:
    path: "./data/joxette.ducklake"
    object-storage-path: "s3://acme-joxette/prod/data"
    # s3 block intentionally absent — use credential chain / IRSA
```

Environment variables (EKS / EC2):

```bash
AWS_DEFAULT_REGION=eu-west-1
# No key/secret needed when using instance profile or IRSA
```

### application.yml — S3-compatible (MinIO / LocalStack)

```yaml
joxette:
  catalog:
    path: "./data/joxette.ducklake"
    object-storage-path: "s3://joxette-dev/data"

  s3:
    endpoint:   "http://localhost:9000"
    access-key: "minioadmin"
    secret-key:  "minioadmin"
    region:     "us-east-1"
```

`USE_SSL false` and `URL_STYLE path` are set automatically whenever
`joxette.s3.endpoint` is non-blank.

---

## Google Cloud Storage

### Path format

DuckDB's `httpfs` extension maps `gs://` to GCS:

```
gs://<bucket>/<prefix>
```

Example:

```
gs://acme-joxette/prod/data
```

### DuckDB secret

```sql
CREATE OR REPLACE SECRET joxette_gcs (
    TYPE            GCS,
    KEY_ID          '<hmac-access-id>',        -- HMAC key from GCS console
    SECRET          '<hmac-secret>'
);
```

GCS HMAC keys are created in the GCS console under
**Storage → Settings → Interoperability**. They are scoped to a service
account.

Alternatively, use Application Default Credentials (ADC) — DuckDB picks
them up automatically when `GOOGLE_APPLICATION_CREDENTIALS` is set or when
running on GCE / GKE with Workload Identity. In that case no explicit
secret is needed.

### Service account permissions

The service account needs the following roles on the bucket:

| Role | Purpose |
|---|---|
| `roles/storage.objectAdmin` | Create, read, delete Parquet files |
| `roles/storage.legacyBucketReader` | List bucket contents |

Using IAM conditions you can restrict to the prefix:

```
resource.name.startsWith("projects/_/buckets/acme-joxette/objects/prod/data/")
```

### application.yml — GCS with HMAC key

Joxette does not yet auto-register a GCS secret the way it does for S3.
Register the secret via DuckDB before Joxette starts, or add a startup SQL
script:

```yaml
joxette:
  catalog:
    path: "./data/joxette.ducklake"
    object-storage-path: "gs://acme-joxette/prod/data"
```

`src/main/resources/db/secrets.sql` (executed by `DuckLakeManager` or a
startup hook — add one if absent):

```sql
CREATE OR REPLACE SECRET joxette_gcs (
    TYPE   GCS,
    KEY_ID '${GCS_HMAC_KEY_ID}',
    SECRET '${GCS_HMAC_SECRET}'
);
```

Environment variables:

```bash
GCS_HMAC_KEY_ID=GOOG1E...
GCS_HMAC_SECRET=xxxxx
```

### application.yml — GCS with Workload Identity (GKE)

```yaml
joxette:
  catalog:
    path: "./data/joxette.ducklake"
    object-storage-path: "gs://acme-joxette/prod/data"
    # No s3/gcs secret block needed — ADC is resolved automatically
```

Annotate the Kubernetes service account with the GCP service account email:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  annotations:
    iam.gke.io/gcp-service-account: joxette@my-project.iam.gserviceaccount.com
```

---

## Azure Blob Storage

### Path format

DuckDB's `httpfs` extension uses the `az://` scheme:

```
az://<container>/<prefix>
```

Example:

```
az://joxette-prod/data
```

The storage account is specified in the secret, not in the path.

### DuckDB secret

```sql
-- Service principal (client credentials)
CREATE OR REPLACE SECRET joxette_azure (
    TYPE            AZURE,
    CONNECTION_STRING 'AccountName=<account>;AccountKey=<base64-key>'
);

-- OR with a SAS token
CREATE OR REPLACE SECRET joxette_azure (
    TYPE            AZURE,
    CONNECTION_STRING 'AccountName=<account>;SharedAccessSignature=<sas-token>'
);

-- OR with managed identity (no credentials in config)
CREATE OR REPLACE SECRET joxette_azure (
    TYPE            AZURE,
    PROVIDER        'credential_chain',
    ACCOUNT_NAME    '<storage-account>'
);
```

### Required permissions

Assign the following built-in roles to the identity (managed identity,
service principal, or user) on the **container** (not the storage account):

| Role | Purpose |
|---|---|
| `Storage Blob Data Contributor` | Read, write, delete blobs |
| `Storage Blob Data Reader` | List and read (if you want least-privilege reads) |

For compaction `Storage Blob Data Contributor` is sufficient as it covers
delete.

### application.yml — Azure with account key

Joxette does not yet auto-register an Azure secret. Register it via a
startup SQL script:

```yaml
joxette:
  catalog:
    path: "./data/joxette.ducklake"
    object-storage-path: "az://joxette-prod/data"
```

`db/secrets.sql`:

```sql
CREATE OR REPLACE SECRET joxette_azure (
    TYPE              AZURE,
    CONNECTION_STRING 'AccountName=${AZURE_STORAGE_ACCOUNT};AccountKey=${AZURE_STORAGE_KEY}'
);
```

Environment variables:

```bash
AZURE_STORAGE_ACCOUNT=acmejoxette
AZURE_STORAGE_KEY=base64==
```

### application.yml — Azure with managed identity (AKS Workload Identity)

```yaml
joxette:
  catalog:
    path: "./data/joxette.ducklake"
    object-storage-path: "az://joxette-prod/data"
```

`db/secrets.sql`:

```sql
CREATE OR REPLACE SECRET joxette_azure (
    TYPE         AZURE,
    PROVIDER     'credential_chain',
    ACCOUNT_NAME '${AZURE_STORAGE_ACCOUNT}'
);
```

The `credential_chain` provider resolves credentials in this order:
environment variables → workload identity → managed identity → Azure CLI.
On AKS with Workload Identity no static credentials are needed.

---

## Local development

No object storage is required. Leave `object-storage-path` blank and
Joxette writes Parquet files to a sibling directory next to the catalog
file (`joxette_data/` by default).

```yaml
joxette:
  catalog:
    path: "./data/joxette.ducklake"
    object-storage-path: ""    # blank = local filesystem
```

For a MinIO container that mirrors a real S3 bucket, see
[`docker-compose.yml`](../docker-compose.yml).

---

## Secret lifecycle

DuckDB secrets created with `CREATE SECRET` are **session-scoped** unless
`PERSISTENT` is specified. Joxette always recreates them on startup
(`DROP SECRET IF EXISTS … ; CREATE SECRET …`) so no state is left between
restarts. Never use `CREATE PERSISTENT SECRET` — it writes credentials to
disk in the DuckDB catalog file.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `HTTP Error 403` on first write | Missing IAM/SA permission | Add `PutObject` / `Storage Blob Data Contributor` |
| `SSL error` with MinIO | `USE_SSL false` not set | Set `joxette.s3.endpoint` to trigger path-style + no-SSL mode |
| `NoSuchBucket` | Bucket does not exist | Create bucket first; DuckLake does not create buckets |
| `InvalidAccessKeyId` on AWS | Wrong credentials or region | Check `AWS_ACCESS_KEY_ID` and `AWS_DEFAULT_REGION` |
| `AuthenticationFailed` on Azure | SAS token expired | Rotate the SAS token or switch to managed identity |
| GCS 401 after pod restart | HMAC key rotated | Update `GCS_HMAC_SECRET` env var and restart |
| DuckLake attaches but writes go to disk | `object-storage-path` blank | Set the path explicitly in production |
