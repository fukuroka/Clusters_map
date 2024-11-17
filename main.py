import os
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from PyQt5.QtWidgets import (
    QApplication, QMainWindow, QWidget, QVBoxLayout, QScrollArea,
    QGridLayout, QLabel, QDialog, QLineEdit, QPushButton
)


# --- Вспомогательные функции --- #

def get_file_clusters(file_path: str) -> List[Tuple[int, int]]:
    """
    Получает информацию о кластерах файла с использованием команды fsutil.

    Args:
        file_path (str): Путь к файлу.

    Returns:
        List[Tuple[int, int]]: Список пар (начальный_кластер, конечный_кластер) для файла.
    """
    try:
        command = subprocess.run(
            ['fsutil', 'file', 'queryextents', file_path],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )

        if command.returncode != 0:
            return []

        output = command.stdout.decode('cp866').strip().replace('/r/n', '/n').split()
        if not output or 'отсутствуют' in ''.join(output):
            return []

        parse_output = [output[i:i + 6] for i in range(0, len(output), 6)]
        result = []
        for cluster_data in parse_output:
            start_cluster = int(cluster_data[-1], 16)
            cluster_count = int(cluster_data[3], 16)
            result.append((start_cluster, start_cluster + cluster_count - 1))

        return result
    except Exception:
        return []


def valid_directory(path: str) -> bool:
    """
    Проверяет, нужно ли сканировать директорию.

    Args:
        path (str): Имя директории.

    Returns:
        bool: True, если директория допустима, иначе False.
    """
    return not (path.startswith('$') or path.startswith('System Volume Information'))


def scan_directory(root: str, file_clusters: Dict[str, List[Tuple[int, int]]]) -> None:
    """
    Рекурсивно сканирует директорию и собирает информацию о кластерах для файлов.

    Args:
        root (str): Путь к корневой директории.
        file_clusters (Dict[str, List[Tuple[int, int]]]): Словарь для сохранения кластеров файлов.
    """
    for entry in os.scandir(root):
        if entry.is_dir() and valid_directory(entry.name):
            scan_directory(entry.path, file_clusters)
        elif entry.is_file():
            clusters = get_file_clusters(entry.path)
            if clusters:
                file_clusters[entry.path] = clusters


def get_files_with_clusters(file_path: str) -> Dict[str, List[Tuple[int, int]]]:
    """
    Сканирует диск, на котором расположен файл, и возвращает информацию о кластерах всех файлов.

    Args:
        file_path (str): Путь к файлу.

    Returns:
        Dict[str, List[Tuple[int, int]]]: Словарь с файлами и их кластерами.
    """
    file_clusters = {}
    disk_name = os.path.splitdrive(file_path)[0] + '\\'

    # Используем ThreadPoolExecutor для параллельного обхода директорий
    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = []
        for entry in os.scandir(disk_name):
            if entry.is_dir() and valid_directory(entry.name):
                futures.append(executor.submit(scan_directory, entry.path, file_clusters))

        # Дожидаемся завершения всех задач
        for future in futures:
            future.result()

    return {Path(path): clusters for path, clusters in file_clusters.items()}


def get_disk_clusters(file_path: str) -> int:
    """
    Получает общее количество кластеров на диске, используя fsutil.

    Args:
        file_path (str): Путь к файлу.

    Returns:
        int: Общее количество кластеров на диске.
    """
    disk = os.path.splitdrive(file_path)[0]
    command = subprocess.run(['fsutil', 'fsinfo', 'ntfsinfo', disk], stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    if command.returncode != 0:
        return 0

    output = command.stdout.decode('cp866').strip().split('\n')
    total_clusters_parse = ''
    for line in output:
        if 'Всего кластеров' in line:
            total_clusters = line.split(':')[1].strip().split()
            for element in total_clusters:
                if element[0] == '(':
                    break
                total_clusters_parse += element

    return int(total_clusters_parse)


# --- Классы для GUI --- #

class ClusterInfoWindow(QDialog):
    """
    Класс для отображения окна информации о кластерах файла.
    """

    def __init__(self, cluster_number: int, file_path: str, cluster_group: List[Tuple[int, int]], parent=None):
        """
        Args:
            cluster_number (int): Номер выбранного кластера.
            file_path (str): Путь к файлу.
            cluster_group (List[Tuple[int, int]]): Группа кластеров файла.
            parent: Родительское окно.
        """
        super().__init__(parent)
        self.setWindowTitle("Детали файла")

        layout = QVBoxLayout(self)
        layout.addWidget(QLabel(f"Файл: {file_path}"))
        layout.addWidget(QLabel(f"Номер кластера в группе: {cluster_number}"))

        scroll_area = QScrollArea(self)
        scroll_content = QWidget()
        scroll_layout = QVBoxLayout(scroll_content)

        for start, end in cluster_group:
            scroll_layout.addWidget(QLabel(f"Кластеры: {start} - {end}"))

        scroll_area.setWidget(scroll_content)
        layout.addWidget(scroll_area)


class Window(QMainWindow):
    """
    Основное окно приложения для отображения и взаимодействия с кластерами.
    """

    def __init__(self):
        """
        Инициализация главного окна.
        """
        super().__init__()
        self.setWindowTitle('Карта кластеров')
        self.setGeometry(580, 300, 600, 400)
        self.central_widget = QWidget(self)
        self.setCentralWidget(self.central_widget)
        self.main_layout = QVBoxLayout(self.central_widget)

        # Поле ввода пути
        self.path_input = QLineEdit(self)
        self.path_input.setPlaceholderText("Введите путь к файлу:")
        self.main_layout.addWidget(self.path_input)

        # Кнопка загрузки кластеров
        self.load_button = QPushButton("Загрузить кластеры", self)
        self.load_button.clicked.connect(self.load_clusters)
        self.main_layout.addWidget(self.load_button)

        # Область для кластеров
        self.scroll_area = QScrollArea(self)
        self.scroll_area_widget = QWidget()
        self.scroll_area.setWidgetResizable(True)
        self.scroll_area.setFixedSize(628, 360)
        self.scroll_area.setWidget(self.scroll_area_widget)
        self.main_layout.addWidget(self.scroll_area)
        self.scroll_area_widget.setContentsMargins(0, 0, 0, 0)

        self.cluster_layout = QGridLayout(self.scroll_area_widget)
        self.cluster_layout.setSpacing(1)
        self.scroll_area_widget.setLayout(self.cluster_layout)

        # Переменные для управления отображением
        self.loaded_clusters = set()
        self.cluster_group = []
        self.file_path = None
        self.file_clusters_map = {}
        self.visible_range = 500
        self.scroll_area.verticalScrollBar().valueChanged.connect(self.on_scroll)
        self.cluster_buttons = []

    def load_clusters(self) -> None:
        """
        Загружает информацию о кластерах для указанного файла.
        """
        file_path = Path(self.path_input.text())
        if os.path.exists(file_path):
            print(f"Загружаем кластеры для: {file_path}")
            self.file_path = file_path
            self.file_clusters_map = get_files_with_clusters(file_path)
            print(f"Загружены кластеры: {self.file_clusters_map}")

            # Получаем кластеры для текущего файла
            self.cluster_group = self.file_clusters_map.get(file_path, [])
            if not self.cluster_group:
                print("Нет кластеров для этого файла")
            self.loaded_clusters.clear()
            self.display_clusters_near_highlighted()
        else:
            print("Неверный путь к файлу")

    def display_clusters_near_highlighted(self) -> None:
        """
        Отображает кластеры, близкие к текущей группе кластеров.
        """
        disk_clusters = get_disk_clusters(self.file_path)
        print(f"Всего кластеров на диске: {disk_clusters}")

        for start, end in self.cluster_group:
            for file_path, clusters in self.file_clusters_map.items():
                for cluster_start, cluster_end in clusters:
                    if start == cluster_start and end == cluster_end:
                        print(f"Отображаем кластеры от {start} до {end} для файла {file_path}")
                        break

            # Отображаем кластеры в заданном диапазоне
            for cluster_index in range(start - self.visible_range, end + self.visible_range + 1):
                if 0 <= cluster_index < disk_clusters and cluster_index not in self.loaded_clusters:
                    self.add_cluster_button(cluster_index)
                    self.loaded_clusters.add(cluster_index)

    def add_cluster_button(self, index: int) -> None:
        """
        Добавляет кнопку для кластера на карту кластеров.

        Args:
            index (int): Индекс кластера.
        """
        # Проверяем, принадлежит ли кластер текущему файлу
        is_file_cluster = any(start <= index <= end for start, end in self.cluster_group)

        # Проверяем, принадлежит ли кластер другим файлам
        is_in_file_clusters_map = any(
            start <= index <= end for file_clusters in self.file_clusters_map.values() for start, end in file_clusters
        )

        # Устанавливаем стиль кнопки в зависимости от типа кластера
        if is_file_cluster:
            btn_style = "background-color: yellow;"
        elif is_in_file_clusters_map:
            btn_style = "background-color: blue;"
        else:
            btn_style = "background-color: gray;"

        # Создаём и добавляем кнопку
        btn = QPushButton()
        btn.setFixedSize(15, 15)
        btn.setStyleSheet(btn_style)
        btn.setProperty("index", index)
        btn.clicked.connect(lambda _, n=index: self.handle_cluster_click(n))

        self.columns_clasters = self.get_columns_count()
        self.cluster_layout.addWidget(btn, index // self.columns_clasters, index % self.columns_clasters)

        self.cluster_buttons.append(btn)
        self.scroll_area_widget.update()

    def get_columns_count(self) -> int:
        """
        Вычисляет количество колонок для отображения кнопок кластеров.

        Returns:
            int: Количество колонок.
        """
        container_width = self.scroll_area.width() - 20
        button_width = 15
        min_margin = 2
        return container_width // (button_width + min_margin) + 2

    def on_scroll(self) -> None:
        """
        Обрабатывает события прокрутки и подгружает дополнительные кластеры.
        """
        disk_clusters = get_disk_clusters(self.file_path)
        scroll_value = self.scroll_area.verticalScrollBar().value()
        max_scroll = self.scroll_area.verticalScrollBar().maximum()

        # Если прокрутка близка к началу или концу, загружаем дополнительные кластеры
        if scroll_value <= max_scroll * 0.1:
            self.load_additional_clusters(above=True)
        elif scroll_value >= max_scroll * 0.9:
            self.load_additional_clusters(above=False)

    def load_additional_clusters(self, above: bool) -> None:
        """
        Загружает дополнительные кластеры при прокрутке.

        Args:
            above (bool): True, если загружаются кластеры выше, иначе False.
        """
        disk_clusters = get_disk_clusters(self.file_path)
        min_loaded = min(self.loaded_clusters) if self.loaded_clusters else 0
        max_loaded = max(self.loaded_clusters) if self.loaded_clusters else 0

        # Определяем диапазон загрузки
        if above:
            start = max(min_loaded - self.visible_range, 0)
            end = min_loaded
        else:
            start = max_loaded + 1
            end = min(max_loaded + self.visible_range, disk_clusters)

        # Добавляем кнопки для кластеров в указанном диапазоне
        for cluster_index in range(start + 1, end + 1):
            if cluster_index not in self.loaded_clusters:
                self.add_cluster_button(cluster_index)
                self.loaded_clusters.add(cluster_index)

    def handle_cluster_click(self, cluster_index: int) -> None:
        """
        Обрабатывает нажатие на кластер.

        Args:
            cluster_index (int): Номер выбранного кластера.
        """
        clicked_file_path = None
        clicked_cluster_group = []

        # Ищем файл, которому принадлежит выбранный кластер
        for file_path, clusters in self.file_clusters_map.items():
            for start, end in clusters:
                if start <= cluster_index <= end:
                    clicked_file_path = file_path
                    clicked_cluster_group = clusters
                    break

        if clicked_file_path:
            if clicked_file_path == self.file_path:  # Кластер из текущего файла
                cluster_info_window = ClusterInfoWindow(
                    cluster_number=cluster_index,
                    file_path=str(clicked_file_path),
                    cluster_group=clicked_cluster_group,
                    parent=self
                )
                cluster_info_window.exec_()
            else:  # Кластер из другого файла
                # Меняем цвет предыдущих жёлтых кластеров на синий
                for btn in self.cluster_buttons:
                    index = btn.property("index")
                    if any(start <= index <= end for start, end in self.cluster_group):
                        btn.setStyleSheet("background-color: blue;")

                # Обновляем текущую группу кластеров и путь к файлу
                self.file_path = clicked_file_path
                self.cluster_group = clicked_cluster_group

                # Обновляем поле ввода пути
                self.path_input.setText(str(clicked_file_path))

                # Подсвечиваем новую группу кластеров
                for btn in self.cluster_buttons:
                    index = btn.property("index")
                    if any(start <= index <= end for start, end in clicked_cluster_group):
                        btn.setStyleSheet("background-color: yellow;")

                print(f"Выделена новая группа кластеров файла: {clicked_file_path}")

if __name__ == '__main__':
    app = QApplication(sys.argv)
    window = Window()
    window.show()
    sys.exit(app.exec_())