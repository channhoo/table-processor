from typing import Optional
from .utils import TableData, SaveError

def save_table(table: TableData, file_path: str):
    if not table.data and not table.columns:
        raise SaveError("Пустая таблица")
    
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            text_repr = _table_to_text(table)
            f.write(text_repr)
    except Exception as e:
        raise SaveError(f"Ошибка сохранения в {file_path}: {str(e)}")

def _table_to_text(table: TableData) -> str:
    if not table.columns:
        return "Пустая таблица\n"
    
    col_widths = []
    for i, col in enumerate(table.columns):
        max_width = len(str(col))
        
        for row in table.data:
            if i < len(row):
                max_width = max(max_width, len(str(row[i])))
        
        col_widths.append(max_width + 2)
    
    lines = []
    
    header = " | ".join(str(col).ljust(col_widths[i]) 
                       for i, col in enumerate(table.columns))
    lines.append(header)
    lines.append("-" * len(header))
    
    for row in table.data:
        row_str = " | ".join(
            str(row[i] if i < len(row) else "").ljust(col_widths[i])
            for i in range(len(table.columns))
        )
        lines.append(row_str)
    
    lines.append(f"\nВсего строк: {len(table.data)}, столбцов: {len(table.columns)}")
    
    return "\n".join(lines)