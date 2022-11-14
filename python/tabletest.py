
class TableTest():

    def __init__(self, table, test, value):
        self.table = table
        self.test = test
        self.value = value

    @property
    def table(self):
        return self._table

    @table.setter
    def table(self, value):
        self._table = value

    @property
    def test(self):
        return self._test

    @test.setter
    def test(self, value):
        self._test = value

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        self._value = value


if __name__ == "__main__":
    pass
