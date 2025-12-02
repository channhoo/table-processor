from typing import List, Dict, Any, Union, Optional
import copy

class TableError(Exception):
    pass

class LoadError(TableError):
    pass

class SaveError(TableError):
    pass

class ColumnError(TableError):
    pass

class OperationError(TableError):
    pass

class TableData:
    def __init__(self, data: Optional[List[List[Any]]] = None, 
                 columns: Optional[List[str]] = None):
        self.data = data if data is not None else []
        self.columns = columns if columns is not None else []
        self.column_types: Dict[Union[int, str], type] = {}
        
        if self.data and self.columns:
            self._infer_column_types()
    
    def _infer_column_types(self):
        if not self.data:
            return
            
        for col_idx in range(len(self.columns)):
            for row in self.data:
                if col_idx < len(row) and row[col_idx] is not None:
                    value = row[col_idx]
                    if isinstance(value, (int, float, bool, str)):
                        self.column_types[col_idx] = type(value)
                        self.column_types[self.columns[col_idx]] = type(value)
                    else:
                        self.column_types[col_idx] = str
                        self.column_types[self.columns[col_idx]] = str
                    break
            else:
                self.column_types[col_idx] = str
                self.column_types[self.columns[col_idx]] = str
    
    def __len__(self):
        return len(self.data)
    
    def __repr__(self):
        return f"TableData(rows={len(self.data)}, columns={len(self.columns)})"