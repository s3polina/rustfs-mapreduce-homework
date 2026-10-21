import duckdb

from common import put_text
from config import DUCK_PREFIX

CSV_PATH = r"D:\Users\polin\rustfs-mapreduce-data\it_vacancies_2024.csv"

VARIANT_SQL = r"""
regexp_matches(
    coalesce(name, '') || ' ' ||
    coalesce(role, '') || ' ' ||
    coalesce(key_skills, '') || ' ' ||
    coalesce(description, ''),
    '(?i)(c\s*#|csharp|\.net\b|dotnet|asp\.net|aspnet)'
)
"""


def connect():
    return duckdb.connect()


def skills_expr():
    return r"""
    list_filter(
        list_transform(
            regexp_split_to_array(
                regexp_replace(
                    coalesce(key_skills, ''),
                    '[\[\]''"]',
                    '',
                    'g'
                ),
                '\s*[;,|]\s*'
            ),
            x -> trim(x)
        ),
        x -> x <> ''
    )
    """


def save_result(df, key):
    put_text(key, df.to_csv(index=False))


def run_duckdb(save=True):
    con = connect()

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW vacancies AS
        SELECT
            row_number() OVER () AS row_id,
            *
        FROM read_csv_auto(
            '{CSV_PATH}',
            all_varchar=true,
            ignore_errors=true
        )
        WHERE {VARIANT_SQL}
    """)

    con.execute(f"""
        CREATE OR REPLACE TEMP VIEW vacancy_skills AS
        SELECT
            row_id,
            list_distinct({skills_expr()}) AS skills
        FROM vacancies
    """)

    q1 = """
        SELECT
            skill_1,
            count(*) AS count
        FROM (
            SELECT
                row_id,
                unnest(skills) AS skill_1
            FROM vacancy_skills
        )
        GROUP BY skill_1
        ORDER BY count DESC, lower(skill_1)
    """

    q2 = """
        WITH s AS (
            SELECT
                row_id,
                row_number() OVER (
                    PARTITION BY row_id
                    ORDER BY lower(skill)
                ) AS rn,
                skill
            FROM (
                SELECT
                    row_id,
                    unnest(skills) AS skill
                FROM vacancy_skills
            )
        )
        SELECT
            a.skill AS skill_1,
            b.skill AS skill_2,
            count(*) AS count
        FROM s a
        JOIN s b
            ON a.row_id = b.row_id
            AND a.rn < b.rn
        GROUP BY a.skill, b.skill
        ORDER BY count DESC, lower(skill_1), lower(skill_2)
    """

    q3 = """
        WITH s AS (
            SELECT
                row_id,
                row_number() OVER (
                    PARTITION BY row_id
                    ORDER BY lower(skill)
                ) AS rn,
                skill
            FROM (
                SELECT
                    row_id,
                    unnest(skills) AS skill
                FROM vacancy_skills
            )
        )
        SELECT
            a.skill AS skill_1,
            b.skill AS skill_2,
            c.skill AS skill_3,
            count(*) AS count
        FROM s a
        JOIN s b
            ON a.row_id = b.row_id
            AND a.rn < b.rn
        JOIN s c
            ON a.row_id = c.row_id
            AND b.rn < c.rn
        GROUP BY a.skill, b.skill, c.skill
        ORDER BY
            count DESC,
            lower(skill_1),
            lower(skill_2),
            lower(skill_3)
    """

    df1 = con.execute(q1).df()
    df2 = con.execute(q2).df()
    df3 = con.execute(q3).df()

    variant_rows = con.execute(
        "SELECT count(*) FROM vacancies"
    ).fetchone()[0]

    if save:
        save_result(df1, f"{DUCK_PREFIX}/skills_1.csv")
        save_result(df2, f"{DUCK_PREFIX}/skills_2.csv")
        save_result(df3, f"{DUCK_PREFIX}/skills_3.csv")

    return {
        "variant_rows": variant_rows,
        "skills_1": df1,
        "skills_2": df2,
        "skills_3": df3,
    }


if __name__ == "__main__":
    result = run_duckdb(save=True)

    print("Вакансий C#/.NET:", result["variant_rows"])

    for r in (1, 2, 3):
        print(f"\nTOP-10, размер стека {r}:")
        print(
            result[f"skills_{r}"]
            .head(10)
            .to_string(index=False)
        )