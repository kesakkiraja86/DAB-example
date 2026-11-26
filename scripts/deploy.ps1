param(
  [ValidateSet("dev","staging","prod")] [string]$Target = "dev",
  [string]$Catalog = "prod_dab"
)
$ErrorActionPreference = "Stop"
Set-Location (Split-Path $PSScriptRoot)

# 1. Catalog (idempotent) - schema and volume are created by the bundle
databricks catalogs get $Catalog 2>$null
if ($LASTEXITCODE -ne 0) { databricks catalogs create $Catalog }

# 2. Deploy schema, volume, job
databricks bundle validate -t $Target
databricks bundle deploy -t $Target

# 3. Upload CSVs into /Volumes/<catalog>/<schema>/<volume>/raw
$schema = (databricks bundle summary -t $Target -o json | ConvertFrom-Json).variables.schema.value
$volume = (databricks bundle summary -t $Target -o json | ConvertFrom-Json).variables.volume.value
$dest = "dbfs:/Volumes/$Catalog/$schema/$volume/raw"
Get-ChildItem data\raw\*.csv | ForEach-Object { databricks fs cp $_.FullName "$dest/$($_.Name)" --overwrite }

# 4. Run the pipeline
databricks bundle run banking_medallion_job -t $Target
