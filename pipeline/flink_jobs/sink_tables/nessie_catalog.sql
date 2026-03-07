CREATE CATALOG nessie_catalog WITH (
    'type'                 = 'iceberg',
    'catalog-impl'         = 'org.apache.iceberg.nessie.NessieCatalog',
    'uri'                  = 'http://nessie:19120/api/v1',
    'ref'                  = 'main',
    'warehouse'            = 's3://lakehouse',
    'io-impl'              = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint'          = 'http://minio:9000',
    's3.access-key-id'     = 'admin',
    's3.secret-access-key' = 'admin123',
    's3.path-style-access' = 'true',
    'client.region'        = 'us-east-1'
);