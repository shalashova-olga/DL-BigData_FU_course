# worker_functions.py
from pathlib import Path
import pandas as pd
from typing import Dict, Tuple, List
from pathlib import Path
import pandas as pd
from typing import Dict, Tuple
from multiprocessing import Queue
from collections import Counter


def worker_file_stats(path: str, out_q: Queue): #функция, которая выполняется в КАЖДОМ дочернем процессе
    """Рабочий процесс: считает `tag_stats_from_part` и складывает результат в очередь."""
    out_q.put(tag_stats_from_part(Path(path)))

#для задачи с совместным разбором
def _worker_count(file_path: str, out_dict, key: str):
    out_dict[key] = count_chars_in_file(file_path)

def count_chars_in_file(file_path: str) -> Dict[str, int]:
    """
    Подсчитать, сколько раз встречается каждый символ в файле.
    Регистр игнорируем. Возвращает словарь {символ: количество}.
    """
    char_counts = Counter()
    with open(file_path, "r", encoding="utf-8") as f:
        for line in f:
            char_counts.update(line.lower())
    # можно убрать перевод строки, если не нужен
    if "\n" in char_counts:
        del char_counts["\n"]
    return dict(char_counts)


# для задачи 4
def tag_stats_from_part(path: Path) -> Dict[str, Tuple[float, int]]:
    """Читает CSV файл и возвращает статистику по тегам"""
    try:
        df = pd.read_csv(path, sep=';')
        stats = {}
        for _, row in df.iterrows():
            tag = row['tag'] 
            nsteps = row['n_steps'] 
            
            if tag not in stats:
                stats[tag] = (nsteps, 1)
            else:
                old_sum, old_count = stats[tag]
                stats[tag] = (old_sum + nsteps, old_count + 1)
        return stats
    except Exception as e:
        print(f"Ошибка при обработке {path}: {e}")
        return {}

def process_single_file(file_path: str):
    """Функция для обработки одного файла"""
    return tag_stats_from_part(Path(file_path))


# для задачи 5
def worker_fixed(in_q: Queue, out_q: Queue):
    """Рабочий процесс: берёт путь к файлу из входной очереди, считает статистику
       и кладёт результат в выходную очередь. Завершается при получении `None`.
    """
    while True:
        path = in_q.get()
        if path is None:
            break  
        out_q.put(tag_stats_from_part(Path(path)))


def merge_tag_stats(stats_list: List[Dict]) -> Dict[str, Tuple[float, int]]:
    """Объединяет статистики из всех файлов"""
    merged = {}
    for stats in stats_list:
        for tag, (sum_val, count) in stats.items():
            if tag not in merged:
                merged[tag] = (sum_val, count)
            else:
                old_sum, old_count = merged[tag]
                merged[tag] = (old_sum + sum_val, old_count + count)
    return merged

