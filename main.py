import pandas as pd
import requests as re
from io import StringIO
import json
from PyQt5.QtWidgets import QApplication, QMainWindow, QGridLayout, QWidget, QTableWidget, QTableWidgetItem
from PyQt5.QtCore import QSize, Qt, QThread, pyqtSignal
import sys


def connect_and_getData():
    s = re.Session()
    try:
        url_auth = 'https://passport.moex.com/authenticate'
        auth = ('rg174@mail.ru', 'Cnfhsq47')
        s.get(url=url_auth, auth=auth)
        headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                 "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/98.0.4758.102 Safari/537.36"}
        cookies = {'MicexPassportCert': s.cookies['MicexPassportCert']}
        url = 'https://iss.moex.com/iss/analyticalproducts/futoi/securities.csv'
        req = re.get(url=url, headers=headers, cookies=cookies)

        data = StringIO(req.text)
        pd.set_option('display.max_columns', None)
        usecols = ['seqnum', 'ticker', 'clgroup', 'pos_long', 'pos_short']
        read_csv = pd.read_csv(data, sep=';', skiprows=2, usecols=usecols).dropna()
        data = read_csv[read_csv['ticker'] == 'RI']
        data2 = data[data['seqnum'] == data['seqnum'].max()]
        fiz = data2[data2['clgroup'] == 'FIZ']
        yur = data2[data2['clgroup'] == 'YUR']

        # data_json = {
        #     "fiz_long_pos": abs(int(fiz['pos_long'])),
        #     "fiz_short_pos": abs(int(fiz['pos_short'])),
        #     "yur_long_pos": abs(int(yur['pos_long'])),
        #     "yur_short_pos": abs(int(yur['pos_short']))
        # }
        # return json.dumps(data_json)
        return [0, 0, abs(int(fiz['pos_long'])), abs(int(fiz['pos_short'])), abs(int(yur['pos_long'])),
                abs(int(yur['pos_short']))]
    except:
        print("Не удалось загрузить данные.... Возможно нестабильное подключение.")
        return []


class DataParser(QThread):
    data_signal = pyqtSignal(list)

    def __init__(self):
        super(DataParser, self).__init__()
        self._date = ''
        self._nameProg = ''
        self._start = ''
        self._flag = True
        self._list = []

    def run(self):
        self.msleep(2000)
        # вставьте в этот цикл свою логику получения списка list_to_add
        # у меня это рандомный _list, который формируется каждые 10 секунд,
        # чтобы вы спокойно могли наблюдать что происходит
        while (self._flag):

            new_list = connect_and_getData()
            # _list = new_list
            if (new_list not in self._list) and (bool(new_list)):
                self._list.append(new_list)
                # print(self._list)
            self.data_signal.emit(self._list)  # отдаем список в основной поток
            self.msleep(4000)

        # Наследуемся от QMainWindow


class MainWindow(QMainWindow):
    # Переопределяем конструктор класса
    def __init__(self):
        # Обязательно нужно вызвать метод супер класса
        QMainWindow.__init__(self)

        self.setMinimumSize(QSize(780, 80))  # Устанавливаем размеры
        self.setWindowTitle("Работа с QTableWidget")  # Устанавливаем заголовок окна
        central_widget = QWidget(self)  # Создаём центральный виджет
        self.setCentralWidget(central_widget)  # Устанавливаем центральный виджет

        grid_layout = QGridLayout()  # Создаём QGridLayout
        central_widget.setLayout(grid_layout)  # Устанавливаем данное размещение в центральный виджет

        self.table = QTableWidget(self)  # Создаём таблицу
        self.table.setColumnCount(6)  # Устанавливаем три колонки
        self.table.setRowCount(1)  # и одну строку в таблице

        # Устанавливаем заголовки таблицы
        self.table.setHorizontalHeaderLabels(["a", "b", "Длинные позиции\n(позиции покупателя)",
                                              "Короткие позиции\n(позиции продавца)",
                                              "Длинные позиции\n(позиции покупателя)",
                                              "Короткие позиции\n(позиции продавца)"])

        # Устанавливаем всплывающие подсказки на заголовки
        self.table.horizontalHeaderItem(0).setToolTip("Column 1 ")
        self.table.horizontalHeaderItem(1).setToolTip("Column 2 ")
        self.table.horizontalHeaderItem(2).setToolTip("Column 3 ")

        # Устанавливаем выравнивание на заголовки
        self.table.horizontalHeaderItem(0).setTextAlignment(Qt.AlignLeft)
        self.table.horizontalHeaderItem(1).setTextAlignment(Qt.AlignHCenter)
        self.table.horizontalHeaderItem(2).setTextAlignment(Qt.AlignRight)

        # заполняем первую строку
        # table.setItem(0, 0, QTableWidgetItem("Text in column 1"))
        # table.setItem(0, 1, QTableWidgetItem("Text in column 2"))
        # table.setItem(0, 2, QTableWidgetItem("Text in column 3"))

        # делаем ресайз колонок по содержимому
        self.table.resizeColumnsToContents()

        grid_layout.addWidget(self.table, 0, 0)  # Добавляем таблицу в сетку

        self.thread = DataParser()
        self.thread.data_signal.connect(self.update_data)
        self.thread.start()

    def update_data(self, _data):
        # print(_data)
        if self.table.rowCount() <= len(_data):
            self.table.insertRow(0)
            self.table.setItem(0, 0, QTableWidgetItem(str(_data[-1][0])))
            self.table.setItem(0, 1, QTableWidgetItem(str(_data[-1][1])))
            self.table.setItem(0, 2, QTableWidgetItem(str(_data[-1][2])))
            self.table.setItem(0, 3, QTableWidgetItem(str(_data[-1][3])))
            self.table.setItem(0, 4, QTableWidgetItem(str(_data[-1][4])))
            self.table.setItem(0, 5, QTableWidgetItem(str(_data[-1][5])))
        # grid_layout = QGridLayout()  # Создаём QGridLayout
        # table = QTableWidget(self)  # Создаём таблицу
        # table.setColumnCount(6)  # Устанавливаем три колонки
        # table.setRowCount(len(_data))  # и одну строку в таблице
        #
        # # Устанавливаем заголовки таблицы
        # table.setHorizontalHeaderLabels(["a", "b", "Длинные позиции\n(позиции покупателя)",
        #                                  "Короткие позиции\n(позиции продавца)",
        #                                  "Длинные позиции\n(позиции покупателя)",
        #                                  "Короткие позиции\n(позиции продавца)"])
        #
        # # Устанавливаем выравнивание на заголовки
        # table.horizontalHeaderItem(0).setTextAlignment(Qt.AlignLeft)
        # table.horizontalHeaderItem(1).setTextAlignment(Qt.AlignHCenter)
        # table.horizontalHeaderItem(2).setTextAlignment(Qt.AlignRight)
        #
        # i = 0
        # for item in _data:
        #     table.setItem(i, 0, QTableWidgetItem(_data[0]))
        #     table.setItem(i, 1, QTableWidgetItem(_data[1]))
        #     table.setItem(i, 2, QTableWidgetItem(_data[2]))
        #     table.setItem(i, 3, QTableWidgetItem(_data[3]))
        #     table.setItem(i, 4, QTableWidgetItem(_data[4]))
        #     table.setItem(i, 5, QTableWidgetItem(_data[5]))
        #     i += 1
        # # делаем ресайз колонок по содержимому
        # table.resizeColumnsToContents()
        #
        # grid_layout.addWidget(table, 0, 0)  # Добавляем таблицу в сетку

        # rowPos = 0
        # self.tableWidget.insertRow(rowPos)
        # self.tableWidget.setItem(rowPos, 2, QTableWidgetItem(_data[0]))
        # self.tableWidget.setItem(rowPos, 3, QTableWidgetItem(_data[1]))
        # self.tableWidget.setItem(rowPos, 4, QTableWidgetItem(_data[2]))
        # self.tableWidget.setItem(rowPos, 5, QTableWidgetItem(_data[2]))
        #
        # rows = self.tableWidget.rowCount()
        # for row in range(1, rows):                    # для rowPos = 0
        #     _data = self.tableWidget.item(row, 3).text()
        #     self.tableWidget.setItem(row, 0, QTableWidgetItem(_data[0]))


if __name__ == '__main__':
    # data = json.loads(connect())
    # print(data["fiz"])
    #
    # with open("data_file.json", "a+") as write_file:
    #     json.dump(data, write_file)

    app = QApplication(sys.argv)
    mw = MainWindow()
    mw.show()
    sys.exit(app.exec())
