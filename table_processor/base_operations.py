from typing import List, Dict, Any, Union, Optional, Tuple
import copy
from .utils import TableData, TableError, ColumnError, OperationError

class TableProcessor:
    def __init__(self, table: Optional[TableData] = None):
        self._table = table if table is not None else TableData()
    
    @property
    def table(self) -> TableData:
        return self._table
    
    def get_rows_by_number(self, start: int, stop: Optional[int] = None, 
                          copy_table: bool = False) -> 'TableProcessor':
        if not self._table.data:
            return TableProcessor(TableData(columns=self._table.columns))
        
        if start < 0 or start >= len(self._table.data):
            raise TableError(f"Некорректный начальный индекс: {start}")
        
        if stop is not None:
            if stop < 0 or stop > len(self._table.data):
                raise TableError(f"Некорректный конечный индекс: {stop}")
            if start >= stop:
                raise TableError(f"Начальный индекс должен быть меньше конечного")
            data_slice = self._table.data[start:stop]
        else:
            data_slice = [self._table.data[start]]
        
        if copy_table:
            new_data = copy.deepcopy(data_slice)
            new_table = TableData(new_data, self._table.columns.copy())
            new_table.column_types = self._table.column_types.copy()
        else:
            new_table = TableData(data_slice, self._table.columns)
            new_table.column_types = self._table.column_types
            if stop is None:
                new_table.data = [self._table.data[start]]
            else:
                new_table.data = self._table.data[start:stop]
        
        return TableProcessor(new_table)
    
    def get_rows_by_index(self, *indices: Any, copy_table: bool = False) -> 'TableProcessor':
        if not self._table.data:
            return TableProcessor(TableData(columns=self._table.columns))
        
        if not indices:
            raise TableError("Не указаны значения для поиска")
        
        selected_data = []
        selected_indices = []
        
        for idx, row in enumerate(self._table.data):
            if row and row[0] in indices:
                selected_data.append(row)
                selected_indices.append(idx)
        
        if copy_table:
            new_data = copy.deepcopy(selected_data)
            new_table = TableData(new_data, self._table.columns.copy())
            new_table.column_types = self._table.column_types.copy()
        else:
            if selected_indices:
                new_table = TableData([], self._table.columns)
                new_table.data = [self._table.data[i] for i in selected_indices]
                new_table.column_types = self._table.column_types
            else:
                new_table = TableData([], self._table.columns.copy())
                new_table.column_types = self._table.column_types.copy()
        
        return TableProcessor(new_table)
    
    def get_column_types(self, by_number: bool = True) -> Dict[Union[int, str], type]:
        if not self._table.column_types:
            return {}
        
        if by_number:
            return {k: v for k, v in self._table.column_types.items() 
                   if isinstance(k, int)}
        else:
            return {k: v for k, v in self._table.column_types.items() 
                   if isinstance(k, str)}
    
    def set_column_types(self, types_dict: Dict[Union[int, str], type], 
                        by_number: bool = True):
        if not self._table.columns:
            raise ColumnError("Таблица не имеет столбцов")
        
        for key, type_val in types_dict.items():
            if type_val not in (int, float, bool, str):
                raise ColumnError(f"Неподдерживаемый тип: {type_val}")
            
            if by_number:
                if isinstance(key, int):
                    if key >= len(self._table.columns):
                        raise ColumnError(f"Некорректный индекс столбца: {key}")
                    self._table.column_types[key] = type_val
                    if key < len(self._table.columns):
                        self._table.column_types[self._table.columns[key]] = type_val
            else:
                if isinstance(key, str):
                    if key not in self._table.columns:
                        raise ColumnError(f"Столбец не найден: {key}")
                    self._table.column_types[key] = type_val
                    col_idx = self._table.columns.index(key)
                    self._table.column_types[col_idx] = type_val
    
    def get_values(self, column: Union[int, str] = 0) -> List[Any]:
        col_idx = self._get_column_index(column)
        
        values = []
        for row in self._table.data:
            if col_idx < len(row):
                value = row[col_idx]
                if col_idx in self._table.column_types:
                    try:
                        value = self._table.column_types[col_idx](value)
                    except (ValueError, TypeError):
                        pass
                values.append(value)
        
        return values
    
    def get_value(self, column: Union[int, str] = 0) -> Any:
        if len(self._table.data) != 1:
            raise TableError("Таблица должна содержать ровно одну строку")
        
        values = self.get_values(column)
        return values[0] if values else None
    
    def set_values(self, values: List[Any], column: Union[int, str] = 0):
        if len(values) != len(self._table.data):
            raise ColumnError(
                f"Количество значений ({len(values)}) не соответствует "
                f"количеству строк ({len(self._table.data)})"
            )
        
        col_idx = self._get_column_index(column)
        
        for i, row in enumerate(self._table.data):
            while len(row) <= col_idx:
                row.append(None)
        
        for i, value in enumerate(values):
            if col_idx in self._table.column_types:
                try:
                    self._table.data[i][col_idx] = self._table.column_types[col_idx](value)
                except (ValueError, TypeError) as e:
                    raise ColumnError(f"Ошибка преобразования значения: {value}") from e
            else:
                self._table.data[i][col_idx] = value
    
    def set_value(self, value: Any, column: Union[int, str] = 0):
        if len(self._table.data) != 1:
            raise TableError("Таблица должна содержать ровно одну строку")
        
        self.set_values([value], column)
    
    def print_table(self):
        if not self._table.columns:
            print("Пустая таблица")
            return
        
        col_widths = []
        for i, col in enumerate(self._table.columns):
            max_width = len(str(col))
            
            for row in self._table.data:
                if i < len(row):
                    max_width = max(max_width, len(str(row[i])))
            
            col_widths.append(max_width + 2)
        
        header = " | ".join(str(col).ljust(col_widths[i]) 
                          for i, col in enumerate(self._table.columns))
        print(header)
        print("-" * len(header))
        
        for row in self._table.data:
            row_str = " | ".join(
                str(row[i] if i < len(row) else "").ljust(col_widths[i])
                for i in range(len(self._table.columns))
            )
            print(row_str)
        
        print(f"\nВсего строк: {len(self._table.data)}, столбцов: {len(self._table.columns)}")
    
    def add(self, col1: Union[int, str], col2: Union[int, str, Any], 
           result_col: Optional[Union[int, str]] = None) -> 'TableProcessor':
        return self._arithmetic_operation(col1, col2, result_col, lambda a, b: a + b, "add")
    
    def sub(self, col1: Union[int, str], col2: Union[int, str, Any],
           result_col: Optional[Union[int, str]] = None) -> 'TableProcessor':
        return self._arithmetic_operation(col1, col2, result_col, lambda a, b: a - b, "sub")
    
    def mul(self, col1: Union[int, str], col2: Union[int, str, Any],
           result_col: Optional[Union[int, str]] = None) -> 'TableProcessor':
        return self._arithmetic_operation(col1, col2, result_col, lambda a, b: a * b, "mul")
    
    def div(self, col1: Union[int, str], col2: Union[int, str, Any],
           result_col: Optional[Union[int, str]] = None) -> 'TableProcessor':
        return self._arithmetic_operation(col1, col2, result_col, lambda a, b: a / b, "div")
    
    def eq(self, col1: Union[int, str], col2: Union[int, str, Any]) -> List[bool]:
        return self._comparison_operation(col1, col2, lambda a, b: a == b)
    
    def ne(self, col1: Union[int, str], col2: Union[int, str, Any]) -> List[bool]:
        return self._comparison_operation(col1, col2, lambda a, b: a != b)
    
    def gr(self, col1: Union[int, str], col2: Union[int, str, Any]) -> List[bool]:
        return self._comparison_operation(col1, col2, lambda a, b: a > b)
    
    def ls(self, col1: Union[int, str], col2: Union[int, str, Any]) -> List[bool]:
        return self._comparison_operation(col1, col2, lambda a, b: a < b)
    
    def ge(self, col1: Union[int, str], col2: Union[int, str, Any]) -> List[bool]:
        return self._comparison_operation(col1, col2, lambda a, b: a >= b)
    
    def le(self, col1: Union[int, str], col2: Union[int, str, Any]) -> List[bool]:
        return self._comparison_operation(col1, col2, lambda a, b: a <= b)
    
    def filter_rows(self, bool_list: List[bool], copy_table: bool = False) -> 'TableProcessor':
        if len(bool_list) != len(self._table.data):
            raise TableError(
                f"Длина bool_list ({len(bool_list)}) должна соответствовать "
                f"количеству строк ({len(self._table.data)})"
            )
        
        filtered_data = []
        for i, keep in enumerate(bool_list):
            if keep and i < len(self._table.data):
                filtered_data.append(self._table.data[i])
        
        if copy_table:
            new_data = copy.deepcopy(filtered_data)
            new_table = TableData(new_data, self._table.columns.copy())
            new_table.column_types = self._table.column_types.copy()
        else:
            new_table = TableData(filtered_data, self._table.columns)
            new_table.column_types = self._table.column_types
        
        return TableProcessor(new_table)
    
    def _get_column_index(self, column: Union[int, str]) -> int:
        if isinstance(column, int):
            if column < 0 or column >= len(self._table.columns):
                raise ColumnError(f"Некорректный индекс столбца: {column}")
            return column
        elif isinstance(column, str):
            if column not in self._table.columns:
                raise ColumnError(f"Столбец не найден: {column}")
            return self._table.columns.index(column)
        else:
            raise ColumnError(f"Некорректный тип столбца: {type(column)}")
    
    def _arithmetic_operation(self, col1: Union[int, str], col2: Union[int, str, Any],
                            result_col: Optional[Union[int, str]], 
                            operation: callable, op_name: str) -> 'TableProcessor':
        col1_idx = self._get_column_index(col1)
        
        values1 = self.get_values(col1_idx)
        
        if isinstance(col2, (int, str)) and col2 in self._table.columns or \
           isinstance(col2, int) and 0 <= col2 < len(self._table.columns):
            col2_idx = self._get_column_index(col2)
            values2 = self.get_values(col2_idx)
        else:
            values2 = [col2] * len(values1)
        
        col1_type = self._table.column_types.get(col1_idx, type(values1[0]) if values1 else None)
        if col1_type not in (int, float, bool):
            raise OperationError(
                f"Операция {op_name} поддерживается только для числовых типов и bool. "
                f"Тип столбца {col1}: {col1_type}"
            )
        
        results = []
        for v1, v2 in zip(values1, values2):
            try:
                if isinstance(v1, bool):
                    v1 = int(v1)
                if isinstance(v2, bool):
                    v2 = int(v2)
                
                if op_name == "div" and v2 == 0:
                    raise OperationError("Деление на ноль")
                
                result = operation(v1, v2)                    
                results.append(result)
            except Exception as e:
                raise OperationError(f"Ошибка операции {op_name}: {e}")
        
        if result_col is None:
            result_idx = col1_idx
        else:
            result_idx = self._get_column_index(result_col)
        
        self.set_values(results, result_idx)
        
        if results:
            result_type = type(results[0])
            self._table.column_types[result_idx] = result_type
            if result_idx < len(self._table.columns):
                self._table.column_types[self._table.columns[result_idx]] = result_type
        
        return self
    
    def _comparison_operation(self, col1: Union[int, str], col2: Union[int, str, Any],
                            operation: callable) -> List[bool]:
        col1_idx = self._get_column_index(col1)
        values1 = self.get_values(col1_idx)
        
        if isinstance(col2, (int, str)) and col2 in self._table.columns or \
           isinstance(col2, int) and 0 <= col2 < len(self._table.columns):
            col2_idx = self._get_column_index(col2)
            values2 = self.get_values(col2_idx)
        else:
            values2 = [col2] * len(values1)
        
        results = []
        for v1, v2 in zip(values1, values2):
            try:
                if isinstance(v1, bool):
                    v1 = int(v1)
                if isinstance(v2, bool):
                    v2 = int(v2)
                
                results.append(operation(v1, v2))
            except Exception as e:
                raise OperationError(f"Ошибка сравнения: {e}")
        
        return results