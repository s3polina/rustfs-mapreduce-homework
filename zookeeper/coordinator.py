import gzip
import logging
import os
import pickle
import sys
import time
from collections import Counter
from pathlib import Path

from kazoo.client import KazooClient
from kazoo.exceptions import BadVersionError, NodeExistsError

sys.path.insert(0, "/app")

from common import normalize_skills, variant_match


COORDINATOR_ID = os.getenv("COORDINATOR_ID", "coord")
ZK_HOSTS = os.getenv(
    "ZK_HOSTS",
    "zoo1:2181,zoo2:2181,zoo3:2181",
)

CSV_PATH = os.getenv(
    "CSV_PATH",
    "/data/it_vacancies_2024.csv",
)

FENCING = os.getenv("FENCING", "0") == "1"
RUN_NAME = os.getenv("RUN_NAME", "nofence")

SESSION_TIMEOUT = 10.0
CHECKPOINT_EVERY = 250
STEP_DELAY = 1.2

ROOT = f"/hw4/{RUN_NAME}"
ELECTION_PATH = f"{ROOT}/election"
RESULT_PATH = f"{ROOT}/result_pointer"

CHECKPOINT_PATH = Path(
    f"/state/checkpoint_{RUN_NAME}.pkl.gz"
)

EXPECTED_PAIR_SUM = 259817
EXPECTED_UNIQUE_PAIRS = 57909


def setup_logger():
    Path("/app/logs").mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(COORDINATOR_ID)
    logger.setLevel(logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s | %(message)s",
        datefmt="%H:%M:%S",
    )

    file_handler = logging.FileHandler(
        f"/app/logs/{RUN_NAME}_{COORDINATOR_ID}.log",
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger


log = setup_logger()


def load_vacancies():
    import csv

    result = []

    log.info("Читаю исходный CSV...")

    with open(
        CSV_PATH,
        "r",
        encoding="utf-8-sig",
        newline="",
        errors="replace",
    ) as file:
        reader = csv.DictReader(file)

        for row in reader:
            if not variant_match(row):
                continue

            skills = sorted(
                set(normalize_skills(row.get("key_skills"))),
                key=lambda x: (x.casefold(), x),
            )

            result.append(skills)

    log.info(
        "Подготовка завершена: найдено %s вакансий C#/.NET",
        len(result),
    )

    return result


def save_checkpoint(index, counter):
    CHECKPOINT_PATH.parent.mkdir(
        parents=True,
        exist_ok=True,
    )

    data = {
        "next_index": index,
        "counter": counter,
    }

    with gzip.open(CHECKPOINT_PATH, "wb") as file:
        pickle.dump(data, file)


def load_checkpoint():
    if not CHECKPOINT_PATH.exists():
        return 0, Counter()

    with gzip.open(CHECKPOINT_PATH, "rb") as file:
        data = pickle.load(file)

    return (
        data["next_index"],
        data["counter"],
    )


def ensure_zookeeper_paths(zk):
    zk.ensure_path(ROOT)
    zk.ensure_path(ELECTION_PATH)

    try:
        zk.create(
            RESULT_PATH,
            b"empty",
            makepath=True,
        )
    except NodeExistsError:
        pass


def publish_result(zk, epoch, result):
    payload = (
        f"coordinator={COORDINATOR_ID};"
        f"pairs={result};"
        f"epoch={epoch}"
    ).encode()

    if not FENCING:
        stat = zk.set(
            RESULT_PATH,
            payload,
        )

        log.info(
            "ЗАПИСАЛ РЕЗУЛЬТАТ: %s | pointer version=%s",
            result,
            stat.version,
        )

        return

    try:
        stat = zk.set(
            RESULT_PATH,
            payload,
            version=epoch,
        )

        log.info(
            "ЗАПИСАЛ РЕЗУЛЬТАТ с fencing: %s | pointer version=%s",
            result,
            stat.version,
        )

    except BadVersionError:
        log.error(
            "BadVersionError: запись отклонена. "
            "Эпоха %s устарела.",
            epoch,
        )


def run_as_leader(zk, vacancies):
    _, pointer_stat = zk.get(RESULT_PATH)

    epoch = pointer_stat.version

    log.info(
        "СТАЛ ЛИДЕРОМ | coordinator=%s | epoch=%s | fencing=%s",
        COORDINATOR_ID,
        epoch,
        int(FENCING),
    )

    start_index, pair_counter = load_checkpoint()

    if start_index > 0:
        log.info(
            "ВОССТАНОВЛЕНИЕ С CHECKPOINT: "
            "продолжаю с вакансии %s из %s",
            start_index,
            len(vacancies),
        )
    else:
        log.info("Checkpoint отсутствует, начинаю с нуля")

    for index in range(
        start_index,
        len(vacancies),
    ):
        skills = vacancies[index]
        n = len(skills)

        for i in range(n):
            for j in range(i + 1, n):
                pair_counter[
                    (skills[i], skills[j])
                ] += 1

        processed = index + 1

        if (
            processed % CHECKPOINT_EVERY == 0
            or processed == len(vacancies)
        ):
            save_checkpoint(
                processed,
                pair_counter,
            )

            log.info(
                "checkpoint: %s/%s | pair_sum=%s",
                processed,
                len(vacancies),
                sum(pair_counter.values()),
            )

            time.sleep(STEP_DELAY)

    pair_sum = sum(pair_counter.values())
    unique_pairs = len(pair_counter)

    log.info(
        "РАСЧЁТ ЗАВЕРШЁН | pair_sum=%s | unique_pairs=%s",
        pair_sum,
        unique_pairs,
    )

    if (
        pair_sum == EXPECTED_PAIR_SUM
        and unique_pairs == EXPECTED_UNIQUE_PAIRS
    ):
        log.info(
            "КОНТРОЛЬ ДЗ3 СОВПАЛ: "
            "259817 / 57909"
        )
    else:
        log.error(
            "КОНТРОЛЬ НЕ СОВПАЛ: ожидалось "
            "%s / %s",
            EXPECTED_PAIR_SUM,
            EXPECTED_UNIQUE_PAIRS,
        )

    publish_result(
        zk,
        epoch,
        pair_sum,
    )

    log.info("Лидер остаётся активным после публикации результата")

    while True:
        time.sleep(1)


def main():
    log.info(
        "Запуск %s | FENCING=%s | session_timeout=10s",
        COORDINATOR_ID,
        int(FENCING),
    )

    vacancies = load_vacancies()

    zk = KazooClient(
        hosts=ZK_HOSTS,
        timeout=SESSION_TIMEOUT,
    )

    def state_listener(state):
        log.info(
            "ZooKeeper state: %s",
            state,
        )

    zk.add_listener(state_listener)

    zk.start()

    ensure_zookeeper_paths(zk)

    election = zk.Election(
        ELECTION_PATH,
        COORDINATOR_ID,
    )

    try:
        election.run(
            lambda: run_as_leader(
                zk,
                vacancies,
            )
        )
    finally:
        zk.stop()
        zk.close()


if __name__ == "__main__":
    main()