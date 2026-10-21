import csv
import io
from collections import Counter

from common import iter_csv_rows_from_s3, normalize_skills, variant_match, put_text
from config import MR_PREFIX

def counter_to_csv(counter, r):
    out = io.StringIO()
    w = csv.writer(out)
    w.writerow([f"skill_{i}" for i in range(1, r + 1)] + ["count"])
    for combo, count in counter.most_common():
        if r == 1:
            combo = (combo,)
        w.writerow([*combo, count])
    return out.getvalue()

def run_mapreduce(save=True):
    c1, c2, c3 = Counter(), Counter(), Counter()
    total = matched = 0

    for row in iter_csv_rows_from_s3():
        total += 1
        if not variant_match(row):
            continue

        matched += 1
        skills = sorted(set(normalize_skills(row.get("key_skills"))), key=str.casefold)

        for s in skills:
            c1[s] += 1

        n = len(skills)
        for i in range(n):
            for j in range(i + 1, n):
                c2[(skills[i], skills[j])] += 1

        for i in range(n):
            for j in range(i + 1, n):
                for k in range(j + 1, n):
                    c3[(skills[i], skills[j], skills[k])] += 1

    if save:
        put_text(f"{MR_PREFIX}/skills_1.csv", counter_to_csv(c1, 1))
        put_text(f"{MR_PREFIX}/skills_2.csv", counter_to_csv(c2, 2))
        put_text(f"{MR_PREFIX}/skills_3.csv", counter_to_csv(c3, 3))

    return {"total_rows": total, "variant_rows": matched, "skills_1": c1, "skills_2": c2, "skills_3": c3}

if __name__ == "__main__":
    result = run_mapreduce(save=True)
    print("Всего строк:", result["total_rows"])
    print("Вакансий C#/.NET:", result["variant_rows"])
    for r in (1, 2, 3):
        print(f"\nTOP-10, размер стека {r}:")
        for key, value in result[f"skills_{r}"].most_common(10):
            print(value, key)
