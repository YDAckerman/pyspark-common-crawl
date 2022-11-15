
class TableTest():

    def __init__(self, table, operator, value):
        self.table = table
        self.operator = operator
        self.value = value

    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, value):
        self._table = value

    @property
    def operator(self):
        return self._operator

    @operator.setter
    def operator(self, value):
        self._operator = value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value


if __name__ == "__main__":
    pass
