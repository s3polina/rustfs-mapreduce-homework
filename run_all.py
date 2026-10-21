from common import ensure_bucket, object_exists
from config import INPUT_KEY
from mapreduce import run_mapreduce
from duckdb_calc import run_duckdb
from compare_results import main as compare_main
from salary_region import run as salary_region_run

def main():
    ensure_bucket()

    if not object_exists(INPUT_KEY):
        raise SystemExit(
            f"В bucket hh-lake нет объекта {INPUT_KEY}. "
            "Сначала загрузите CSV из прошлого ДЗ."
        )

    print("1/4 MapReduce")
    run_mapreduce(save=True)

    print("\n2/4 DuckDB")
    run_duckdb(save=True)

    print("\n3/4 Сверка")
    compare_main()

    print("\n4/4 Зарплаты и регионы")
    salary_region_run()

    print("\nГотово. Теперь отдельно запустите: python profile_all.py")

if __name__ == "__main__":
    main()
