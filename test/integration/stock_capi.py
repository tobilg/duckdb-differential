"""Minimal test-only ctypes binding to the unmodified stock DuckDB library."""
import ctypes as C
import os
from paths import DUCKDB_LIBRARY

class Result(C.Structure):
    _fields_=[('columns',C.c_uint64),('rows',C.c_uint64),('changed',C.c_uint64),
              ('column_data',C.c_void_p),('error',C.c_char_p),('internal',C.c_void_p)]

class Host:
    def __init__(self):
        self.lib=C.CDLL(DUCKDB_LIBRARY, mode=C.RTLD_GLOBAL)
        p=C.c_void_p; pp=C.POINTER(p)
        signatures={
            'duckdb_create_config':([pp],C.c_int), 'duckdb_set_config':([p,C.c_char_p,C.c_char_p],C.c_int),
            'duckdb_destroy_config':([pp],None),'duckdb_open_ext':([C.c_char_p,pp,p,C.POINTER(C.c_char_p)],C.c_int),
            'duckdb_connect':([p,pp],C.c_int),'duckdb_disconnect':([pp],None),'duckdb_close':([pp],None),
            'duckdb_query':([p,C.c_char_p,C.POINTER(Result)],C.c_int),
            'duckdb_result_error':([C.POINTER(Result)],C.c_char_p),
            'duckdb_destroy_result':([C.POINTER(Result)],None),
            'duckdb_row_count':([C.POINTER(Result)],C.c_uint64),
            'duckdb_value_int64':([C.POINTER(Result),C.c_uint64,C.c_uint64],C.c_int64),
            'duckdb_interrupt':([p],None),
        }
        for name,(args,result) in signatures.items():
            f=getattr(self.lib,name);f.argtypes=args;f.restype=result
        config=p();assert self.lib.duckdb_create_config(C.byref(config))==0
        assert self.lib.duckdb_set_config(config,b'allow_unsigned_extensions',b'true')==0
        self.db=p();error=C.c_char_p()
        try:
            if self.lib.duckdb_open_ext(None,C.byref(self.db),config,C.byref(error)):
                raise RuntimeError(error.value)
        finally:self.lib.duckdb_destroy_config(C.byref(config))
        self.connections=[]
    def connect(self):
        conn=C.c_void_p();assert self.lib.duckdb_connect(self.db,C.byref(conn))==0
        self.connections.append(conn);return conn
    def query(self,conn,sql):
        result=Result()
        try:
            if self.lib.duckdb_query(conn,sql.encode(),C.byref(result)):
                raise RuntimeError(self.lib.duckdb_result_error(C.byref(result)).decode())
            return self.lib.duckdb_value_int64(C.byref(result),0,0) if self.lib.duckdb_row_count(C.byref(result)) else None
        finally:self.lib.duckdb_destroy_result(C.byref(result))
    def close(self):
        for conn in self.connections:self.lib.duckdb_disconnect(C.byref(conn))
        self.lib.duckdb_close(C.byref(self.db))
