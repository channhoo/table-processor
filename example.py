from table_processor import TableProcessor, TableData, load_csv, save_csv

data = [
    [1, "Alice", 25, 50000.5, True],
    [2, "Bob", 30, 60000.0, False],
    [3, "Charlie", 35, 75000.75, True]
]
columns = ["id", "name", "age", "salary", "active"]

table = TableData(data, columns)
processor = TableProcessor(table)

print("=== Исходная таблица ===")
processor.print_table()

print("\n=== Первые 2 строки ===")
processor.get_rows_by_number(0, 2).print_table()

print("\n=== Увеличение зарплаты на 10% ===")
processor.mul("salary", 1.1)
processor.print_table()

print("\n=== Фильтр: возраст > 30 ===")
mask = processor.gr("age", 30)
filtered = processor.filter_rows(mask)
filtered.print_table()

print("\n=== Сохранение в CSV ===")
save_csv(table, "example_output.csv")
print("Файл сохранен: example_output.csv")