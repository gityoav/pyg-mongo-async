# -*- coding: utf-8 -*-

from pyg_mongo_async._async_reader import mongo_async_reader
from pyg_mongo_async._async_cursor import mongo_async_cursor, mongo_async_pk_cursor
from pyg_mongo_async._db_acell import db_acell, db_asave, db_aload, acell_push
from motor import MotorClient

from pyg_mongo import mongo_table
from pyg_base import get_cache
get_cache('mongo_table').update({'aw': (mongo_async_cursor, mongo_async_pk_cursor, MotorClient),
                                 'ar': (mongo_async_reader, mongo_async_reader, MotorClient)})

mongo_table('test', 'test', mode='aw', pk = 'key')
