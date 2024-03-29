from pyg_base import  dictable, as_list, is_int, is_dict, is_strs, is_str, sort, waiter
from pyg_mongo import q, mongo_base_reader
import datetime
from pyg_mongo_async._db_acell import _deleted, _doc, _id, _set

def _dict1(keys):
    if keys is None or is_dict(keys):
        return keys
    else:
        return dict([(key[1:], -1) if key.startswith('-') else (key,1) for key in as_list(keys)])

def _items1(keys):
    return list(_dict1(keys).items()) if keys else []


class mongo_async_reader(mongo_base_reader):
    """
    An asynchronous class accessing the mongodb.
    It is built using the Motor asynchronous interface (https://motor.readthedocs.io/en/stable/)
    
    We cannot override some operations so for example:
        
        sync world            async world                what it does
        ----------            ------------               ------------
        del cursor['key']     await c.delete('key')      deletes key from documents in cursor
        cursor['key'] = val   await c.set(key = val)     set a key in documents in cursor
        len(cursor)           await c.count()            cursor documents count 
        cursor.key            await c.distinct('key')    distinct values (as a list) of a partical key
        
    
    Apart from these interface differences, we really tried to keep the interfaces similar to the synced reader.
    
    """
    async def count(self):
        return await self.collection.count_documents(self._spec)

    async def _assert_one_or_none(self):
        n = await self.count()
        if n>1:
            if self.pk:
                pk = self._pk
                for key in pk:
                    multiple_values = await self.distinct(key)
                    if len(multiple_values) > 1:
                        raise KeyError('too many %s = %s found'%(key, multiple_values))
                self = await self.dedup()
                n = await self.count()
                if n <= 1:
                    return n
            raise ValueError('%s\nNon-unique %i documents %s... e.g. \n%s'%(self.collection, n, self._spec, await self.read(slice(None,3,None))))
        else:
            return n

    async def _assert_unique(self):
        n = await self._assert_one_or_none()
        if n == 0:
            raise ValueError('%s\nNo documents %s'%(self.collection, self._spec))
        return self

    async def find_one(self, doc = None, *args, **kwargs):
        res = self.find(*args, **kwargs)
        if doc:
            res = res.find(self._id(doc))
        return await res._assert_unique()

    async def read_one(self, doc = None, *args, **kwargs):
        reader = kwargs.pop('reader', None)
        record = await self.find_one(doc, *args, **kwargs)
        return await record.read(0, reader = reader)

    async def read(self, item = 0, reader = None):
        """
        reads the next document from the collection.

        :Parameters:
        ----------
        item : int, optional
            Please read the ith record. The default is 0.
        reader : callable/list of callables, optional
            When we read the document from the collection, we first transform them. 
            The default behaviour is to use pyg.base._encode.decode but you may pass reader = False to grab the raw data from mongo

        :Returns:
        -------
        document
            The document from Mongo
            
        item = 0

        """
        cursor = self.cursor
        if is_int(item):
            if item < 0:
                item = await self.count() + item
            if item > 0:
                cursor = cursor.skip(item)
            doc = await cursor.next()
            return self._read(doc, reader = reader)
        elif is_dict(item):
            res = await self.find_one(item)
            doc = await res.cursor.next()
            return self._read(doc, reader = reader)
        elif isinstance(item, slice):
            if not item.step in (None,1):
                raise ValueError('async reader only supports reading without gaps')
            start = item.start or 0
            stop = item.stop or await self.count()            
            if start:
                cursor = cursor.skip(item.start)
            docs = await cursor.to_list(stop - start)
            return dictable([self._read(doc, reader = reader) for doc in docs])
        elif isinstance(item, (list, range, tuple)):
            docs = await waiter([self.read(i, reader) for i in item])
            return docs
    
    async def __getitem__(self, item):
        if is_str(item):
            return await self.distinct(item)
        elif is_strs(as_list(item)):
            return self(projection = as_list(item))
        else:
            return await self.read(item)
    
    async def distinct(self, key):
        """
        returns the distinct cursor values of the key        
        """
        res = await self.cursor.distinct(key)
        try:
            return sort(res)
        except TypeError:
            return res

    async def list(self):
        docs = await self.cursor.to_list(await self.count())
        return [self._read(doc) for doc in docs]

    async def docs(self, *keys, doc = _doc):
        """
        await reader[::] will flattens the entire documents returning a table of their values
        At times, we want to see the full documents, indexed by keys and docs does that.        
        returns a dictable with both keys and the document in the 'doc' column
        
        """
        keys = self._pk + as_list(keys)
        docs = await self.list()
        res = dictable([{key: d.get(key) for key in keys} for d in docs])
        res[doc] = docs
        return res

    async def create_index(self, *keys):
        keys = as_list(keys) or self._pk
        if len(keys):
            return await self.collection.create_index(_items1(keys))
        return self
        
    async def dedup(self):
        """
        Although in principle, if a single process reads/writes to Mongo, we should not get duplicates. 
        In practice, when multiple clients access the database, we occasionally get multiple records with the same primary keys.
        When this happens, we also end up with poor mongo _ids 

        Returns
        -------
        mongo_pk_cursor
            Hopefully, a table with unique keys.

        """
        if self.pk:
            pk = self._pk
            bad = await self(projection = pk)[::]
            bad = bad.sort(_id).listby(pk).inc(lambda _id: len(_id)>1) 
            if len(bad):
                await self.collection.update_many(q._id == sum(bad[lambda _id: _id[:-1]], []), {_set: {_deleted : datetime.datetime.now()}})
        return self

    async def keys(self, item = 0):
        doc = await self.read(item)
        return doc.keys()

