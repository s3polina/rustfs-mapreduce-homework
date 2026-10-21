import csv
import io
import math
from collections import Counter, defaultdict
from statistics import median

from common import iter_csv_rows_from_s3, normalize_skills, variant_match, put_text
from config import ANALYSIS_PREFIX, MIN_COMBO_COUNT

RUB = {"RUR", "RUB"}

def salary_value(row):
    currency = str(row.get("currency", "") or "").upper().strip()
    if currency and currency not in RUB:
        return None

    def num(v):
        try:
            x = float(str(v).replace(",", "."))
            return x if math.isfinite(x) else None
        except Exception:
            return None

    lo = num(row.get("salary_from"))
    hi = num(row.get("salary_to"))
    if lo is not None and hi is not None:
        return (lo + hi) / 2
    return lo if lo is not None else hi

def to_csv(rows, fields):
    out = io.StringIO()
    w = csv.DictWriter(out, fieldnames=fields)
    w.writeheader()
    w.writerows(rows)
    return out.getvalue()

def run():
    vacancies = []
    skill_freq = Counter()
    skill_salaries = defaultdict(list)

    for row in iter_csv_rows_from_s3():
        if not variant_match(row):
            continue
        skills = sorted(set(normalize_skills(row.get("key_skills"))), key=str.casefold)
        sal = salary_value(row)
        area = str(row.get("area", "") or "").strip() or "Не указан"
        vacancies.append((skills, sal, area))

        for s in skills:
            skill_freq[s] += 1
            if sal is not None:
                skill_salaries[s].append(sal)

    all_results = {}
    for r in (2, 3):
        combo_counts = Counter()
        combo_salaries = defaultdict(list)

        for skills, sal, _ in vacancies:
            n = len(skills)
            if r == 2:
                combos = [(skills[i], skills[j]) for i in range(n) for j in range(i+1, n)]
            else:
                combos = [(skills[i], skills[j], skills[k])
                          for i in range(n) for j in range(i+1, n) for k in range(j+1, n)]

            for combo in combos:
                combo_counts[combo] += 1
                if sal is not None:
                    combo_salaries[combo].append(sal)

        rows = []
        for combo, count in combo_counts.items():
            salaries = combo_salaries[combo]
            if count < MIN_COMBO_COUNT or len(salaries) < MIN_COMBO_COUNT:
                continue

            base = max(combo, key=lambda s: skill_freq[s])
            if len(skill_salaries.get(base, [])) < MIN_COMBO_COUNT:
                continue

            combo_med = median(salaries)
            base_med = median(skill_salaries[base])

            row = {f"skill_{i+1}": s for i, s in enumerate(combo)}
            row.update({
                "count": count,
                "salary_count": len(salaries),
                "median_salary": round(combo_med, 2),
                "base_skill": base,
                "base_skill_count": skill_freq[base],
                "base_median_salary": round(base_med, 2),
                "delta_salary": round(combo_med - base_med, 2),
            })
            rows.append(row)

        rows.sort(key=lambda x: x["delta_salary"], reverse=True)
        all_results[r] = rows

        fields = [f"skill_{i}" for i in range(1, r + 1)] + [
            "count", "salary_count", "median_salary",
            "base_skill", "base_skill_count", "base_median_salary", "delta_salary"
        ]
        put_text(f"{ANALYSIS_PREFIX}/salary_effect_{r}.csv", to_csv(rows, fields))

    region_count = Counter(area for _, _, area in vacancies)
    top_regions = [name for name, _ in region_count.most_common(3)]
    region_rows = []

    for area in top_regions:
        subset = [(skills, sal) for skills, sal, a in vacancies if a == area]
        salaries = [sal for _, sal in subset if sal is not None]

        skill_counter = Counter()
        pair_counter = Counter()
        triple_counter = Counter()

        for skills, _ in subset:
            for s in skills:
                skill_counter[s] += 1
            n = len(skills)
            for i in range(n):
                for j in range(i+1, n):
                    pair_counter[(skills[i], skills[j])] += 1
            for i in range(n):
                for j in range(i+1, n):
                    for k in range(j+1, n):
                        triple_counter[(skills[i], skills[j], skills[k])] += 1

        region_rows.append({
            "region": area,
            "vacancies": len(subset),
            "salary_vacancies": len(salaries),
            "median_salary": round(median(salaries), 2) if salaries else "",
            "top_skill": skill_counter.most_common(1)[0][0] if skill_counter else "",
            "top_pair": " + ".join(pair_counter.most_common(1)[0][0]) if pair_counter else "",
            "top_triple": " + ".join(triple_counter.most_common(1)[0][0]) if triple_counter else "",
        })

    put_text(
        f"{ANALYSIS_PREFIX}/regions.csv",
        to_csv(region_rows, ["region", "vacancies", "salary_vacancies", "median_salary", "top_skill", "top_pair", "top_triple"])
    )

    print("Три региона:")
    for x in region_rows:
        print(x)

    for r in (2, 3):
        rows = all_results[r]
        print(f"\nTOP-5 повышающих зарплату, размер {r}:")
        for x in rows[:5]:
            print(x)
        print(f"\nTOP-5 понижающих зарплату, размер {r}:")
        for x in rows[-5:]:
            print(x)

if __name__ == "__main__":
    run()
