S3_ENDPOINT = "http://localhost:9000"
S3_ACCESS_KEY = "rustfsadmin"
S3_SECRET_KEY = "rustfsadmin"

BUCKET = "hh-lake"
INPUT_KEY = "it_vacancies_2024.csv"

MR_PREFIX = "results/mapreduce"
DUCK_PREFIX = "results/duckdb"
ANALYSIS_PREFIX = "results/analysis"

S3_CHUNK_BYTES = 16 * 1024 * 1024
MIN_COMBO_COUNT = 5

VARIANT_REGEX = r"(?i)(c\s*#|csharp|\.net\b|dotnet|asp\.net|aspnet)"
