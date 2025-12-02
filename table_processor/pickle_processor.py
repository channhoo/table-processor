import pickle
from typing import List, Optional
import os
from .utils import TableData, LoadError, SaveError

def load_table(*file_paths) -> TableData:
    if not file_paths:
        raise LoadError("Не указаны файлы для загрузки")
    
    all_data = []
    columns = None
    
    for file_idx, file_path in enumerate(file_paths):
        if not os.path.exists(file_path):
            raise LoadError(f"Файл не существует: {file_path}")
        
        try:
            with open(file_path, 'rb') as f:
                table_part = pickle.load(f)
                
                if not isinstance(table_part, TableData):
                    raise LoadError(f"Файл {file_path} не содержит TableData")
                
                if file_idx == 0:
                    columns = table_part.columns
                    all_data = table_part.data.copy()
                else:
                    if table_part.columns != columns:
                        raise LoadError(
                            f"Несоответствие столбцов в файле {file_path}"
                        )
                    all_data.extend(table_part.data)
                    
        except Exception as e:
            raise LoadError(f"Ошибка чтения файла {file_path}: {str(e)}")
    
    result = TableData(all_data, columns)
    
    if file_paths:
        with open(file_paths[0], 'rb') as f:
            first_table = pickle.load(f)
            result.column_types = first_table.column_types.copy()
    
    return result

def save_table(table: TableData, file_path: str, max_rows: Optional[int] = None):
    if not table.data and not table.columns:
        raise SaveError("Пустая таблица")
    
    if max_rows is None or len(table.data) <= max_rows:
        _save_pickle(table, file_path)
    else:
        base_name, ext = os.path.splitext(file_path)
        
        total_rows = len(table.data)
        num_files = (total_rows + max_rows - 1) // max_rows
        
        for i in range(num_files):
            start_idx = i * max_rows
            end_idx = min((i + 1) * max_rows, total_rows)
            
            partial_table = TableData(
                data=table.data[start_idx:end_idx],
                columns=table.columns
            )
            partial_table.column_types = table.column_types.copy()
            
            partial_path = f"{base_name}_part{i+1}{ext}"
            _save_pickle(partial_table, partial_path)

def _save_pickle(table: TableData, file_path: str):
    try:
        with open(file_path, 'wb') as f:
            pickle.dump(table, f)
    except Exception as e:
        raise SaveError(f"Ошибка сохранения в {file_path}: {str(e)}")