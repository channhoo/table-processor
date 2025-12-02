import os
from table_processor import (
    TableProcessor, TableData,
    load_csv, save_csv,
    load_pickle, save_pickle,
    save_text
)

def test_basic_operations():
    print("=== Тест базовых операций ===")
    
    data = [
        [1, "Alice", 25, 50000.5, True],
        [2, "Bob", 30, 60000.0, False],
        [3, "Charlie", 35, 75000.75, True]
    ]
    columns = ["id", "name", "age", "salary", "active"]
    
    table = TableData(data, columns)
    processor = TableProcessor(table)
    
    print("1. Исходная таблица:")
    processor.print_table()
    
    print("\n2. Первые 2 строки:")
    processor.get_rows_by_number(0, 2).print_table()
    
    print("\n3. Строки с id 1 и 3:")
    processor.get_rows_by_index(1, 3).print_table()
    
    print("\n4. Типы столбцов:")
    print(processor.get_column_types())
    
    print("\n5. Значения столбца 'age':")
    print(processor.get_values("age"))
    
    return True

def test_arithmetic_operations():
    print("\n=== Тест арифметических операций ===")
    
    data = [[10], [20], [30]]
    columns = ["value"]
    
    table = TableData(data, columns)
    processor = TableProcessor(table)
    
    print("Исходные значения:", processor.get_values("value"))
    
    processor.add("value", 5)
    print("После add(5):", processor.get_values("value"))
    
    processor.mul("value", 2)
    print("После mul(2):", processor.get_values("value"))
    
    return True

def test_comparison_operations():
    print("\n=== Тест операций сравнения ===")
    
    data = [[10], [20], [30], [40]]
    columns = ["score"]
    
    table = TableData(data, columns)
    processor = TableProcessor(table)
    
    mask = processor.gr("score", 25)
    print("Маска (score > 25):", mask)
    
    filtered = processor.filter_rows(mask)
    print("Отфильтрованные строки:")
    filtered.print_table()
    
    return True

def test_csv_operations():
    print("\n=== Тест CSV операций ===")
    
    data = [
        [1, "Test1", 100],
        [2, "Test2", 200],
        [3, "Test3", 300]
    ]
    columns = ["id", "name", "value"]
    
    table = TableData(data, columns)
    
    save_csv(table, "test_data.csv")
    print("1. CSV файл сохранен")
    
    loaded = load_csv("test_data.csv")
    processor = TableProcessor(loaded)
    print("2. Загруженная таблица:")
    processor.print_table()
    
    if os.path.exists("test_data.csv"):
        os.remove("test_data.csv")
    
    return True

def test_multiple_files():
    print("\n=== Тест нескольких файлов ===")
    
    data = [
        [1, "A"],
        [2, "B"],
        [3, "C"],
        [4, "D"],
        [5, "E"]
    ]
    columns = ["id", "letter"]
    
    table = TableData(data, columns)
    
    save_csv(table, "multi_test.csv", max_rows=2)
    
    files = ["multi_test_part1.csv", "multi_test_part2.csv", "multi_test_part3.csv"]
    print(f"Создано файлов: {len(files)}")
    
    for file in files:
        if os.path.exists(file):
            os.remove(file)
    
    return True

def test_exceptions():
    print("\n=== Тест исключений ===")
    
    data = [[1], [2], [3]]
    columns = ["test"]
    table = TableData(data, columns)
    processor = TableProcessor(table)
    
    try:
        processor.get_rows_by_number(10)
        print("❌ Должна быть ошибка при некорректном индексе")
        return False
    except Exception as e:
        print(f"✓ Корректная ошибка при get_rows_by_number(10): {type(e).__name__}")
    
    try:
        processor.div("test", 0)
        print("❌ Должна быть ошибка при делении на ноль")
        return False
    except Exception as e:
        print(f"✓ Корректная ошибка при делении на ноль: {type(e).__name__}")
    
    return True

def run_all_tests():
    print("=" * 50)
    print("ЗАПУСК ТЕСТОВ БИБЛИОТЕКИ TABLE PROCESSOR")
    print("=" * 50)
    
    tests = [
        test_basic_operations,
        test_arithmetic_operations,
        test_comparison_operations,
        test_csv_operations,
        test_multiple_files,
        test_exceptions
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append((test.__name__, "PASS" if result else "FAIL"))
        except Exception as e:
            results.append((test.__name__, f"ERROR: {e}"))
    
    print("\n" + "=" * 50)
    print("РЕЗУЛЬТАТЫ ТЕСТОВ:")
    print("=" * 50)
    
    for test_name, status in results:
        print(f"{test_name}: {status}")
    
    all_passed = all("PASS" in str(status) for _, status in results)
    print(f"\nВсе тесты пройдены: {'ДА' if all_passed else 'НЕТ'}")
    
    return all_passed

if __name__ == "__main__":
    run_all_tests()