import csv
from typing import List, Optional, Union
import os
from .utils import TableData, LoadError, SaveError

def load_table(*file_paths, **kwargs) -> TableData:
    if not file_paths:
        raise LoadError("Не указаны файлы для загрузки")
    
    all_data = []
    columns = None
    delimiter = kwargs.get('delimiter', ',')
    
    for file_idx, file_path in enumerate(file_paths):
        if not os.path.exists(file_path):
            raise LoadError(f"Файл не существует: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f, delimiter=delimiter)
                file_data = list(reader)
                
                if not file_data:
                    continue
                
                if file_idx == 0:
                    columns = file_data[0] if file_data else []
                    all_data.extend(file_data[1:] if len(file_data) > 1 else [])
                else:
                    if file_data[0] != columns:
                        raise LoadError(
                            f"Несоответствие столбцов в файле {file_path}. "
                            f"Ожидалось: {columns}, получено: {file_data[0]}"
                        )
                    all_data.extend(file_data[1:] if len(file_data) > 1 else [])
                    
        except Exception as e:
            raise LoadError(f"Ошибка чтения файла {file_path}: {str(e)}")
    
    if columns is None:
        columns = []
    
    converted_data = []
    for row in all_data:
        converted_row = []
        for i, val in enumerate(row):
            if val == '':
                converted_row.append(None)
            else:
                try:
                    if val.isdigit() or (val[0] == '-' and val[1:].isdigit()):
                        converted_row.append(int(val))
                    elif val.replace('.', '', 1).isdigit() or \
                         (val[0] == '-' and val[1:].replace('.', '', 1).isdigit()):
                        converted_row.append(float(val))
                    elif val.lower() in ('true', 'false'):
                        converted_row.append(val.lower() == 'true')
                    else:
                        converted_row.append(val)
                except:
                    converted_row.append(val)
        
        converted_data.append(converted_row)
    
    return TableData(converted_data, columns)

def save_table(table: TableData, file_path: str, max_rows: Optional[int] = None, **kwargs):
    if not table.data and not table.columns:
        raise SaveError("Пустая таблица")
    
    delimiter = kwargs.get('delimiter', ',')
    
    if max_rows is None or len(table.data) <= max_rows:
        _save_csv(table, file_path, delimiter)
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
            _save_csv(partial_table, partial_path, delimiter)

def _save_csv(table: TableData, file_path: str, delimiter: str):
    try:
        with open(file_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f, delimiter=delimiter)
            
            writer.writerow(table.columns)
            
            for row in table.data:
                writer.writerow(row)
                
    except Exception as e:
        raise SaveError(f"Ошибка сохранения в {file_path}: {str(e)}")