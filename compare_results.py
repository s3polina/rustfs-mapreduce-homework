import io
import pandas as pd

from common import get_text


def load_csv(key):
    return pd.read_csv(io.StringIO(get_text(key)))


def normalize(df, r):
    skill_cols = [f"skill_{i}" for i in range(1, r + 1)]

    for col in skill_cols:
        df[col] = df[col].astype(str).str.strip()

    if r > 1:
        normalized_rows = []

        for _, row in df.iterrows():
            skills = [row[col] for col in skill_cols]

            skills = sorted(
                skills,
                key=lambda x: (x.casefold(), x)
            )

            new_row = {
                skill_cols[i]: skills[i]
                for i in range(r)
            }

            new_row["count"] = row["count"]

            normalized_rows.append(new_row)

        df = pd.DataFrame(normalized_rows)

        df = (
            df.groupby(skill_cols, as_index=False)["count"]
            .sum()
        )

    return (
        df
        .sort_values(skill_cols)
        .reset_index(drop=True)
    )

def main():
    for r in (1, 2, 3):
        mr = normalize(
            load_csv(f"results/mapreduce/skills_{r}.csv"),
            r,
        )

        duck = normalize(
            load_csv(f"results/duckdb/skills_{r}.csv"),
            r,
        )

        skill_cols = [
            f"skill_{i}"
            for i in range(1, r + 1)
        ]

        merged = mr.merge(
            duck,
            on=skill_cols,
            how="outer",
            suffixes=("_mr", "_duck"),
        )

        merged["count_mr"] = (
            merged["count_mr"]
            .fillna(0)
            .astype(int)
        )

        merged["count_duck"] = (
            merged["count_duck"]
            .fillna(0)
            .astype(int)
        )

        bad = merged[
            merged["count_mr"] != merged["count_duck"]
        ].copy()

        bad["difference"] = (
            bad["count_duck"]
            - bad["count_mr"]
        )

        print()
        print("=" * 70)
        print(f"РАЗМЕР СТЕКА {r}")
        print("=" * 70)

        print(
            "MapReduce строк:",
            len(mr),
            "| DuckDB строк:",
            len(duck),
        )

        print(
            "Сумма MR:",
            mr["count"].sum(),
            "| DuckDB:",
            duck["count"].sum(),
        )

        print(
            "Количество несовпадающих комбинаций:",
            len(bad),
        )

        print("\nПервые 30 различий:")

        print(
            bad.sort_values(
                "difference",
                key=abs,
                ascending=False,
            )
            .head(30)
            .to_string(index=False)
        )


if __name__ == "__main__":
    main()