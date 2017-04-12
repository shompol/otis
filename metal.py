#!/usr/bin/env python
import os
os.environ.pop('TZ', None)  # call this first before some library somewhere used datetime with bad windoz timezone

import logging
from sqlalchemy import Column, Integer, BigInteger, String, Boolean, orm, ForeignKey, create_engine, Table, select, func, between, Index
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship, deferred
from sqlalchemy.schema import ForeignKeyConstraint
from sqlalchemy.orm.exc import NoResultFound
from lazy import lazy
import aig
import metabase as m


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
Base = declarative_base()

    
class Cdb(Base):
    """
    We use this to load databases or work with a signle db from a server-independent source, like metabase
    """
    __tablename__ = 'client_databases'
    id     = Column(String(20), primary_key=True)
    server = Column(String(20), nullable=False)
    name   = Column(String(50))
    oldid  = Column(Integer, nullable=False)
        
    def __repr__(self):
        return """{}( {}  {}  {}  {} )""".format(self.__class__.__name__, self.server, self.id, self.name, self.oldid)

    
    class NotFound(Exception):
        def __str__(self):
            return "No such database: {}".format(self.args)
            
    def __hash__(self):
        return hash(self.id.lower())

        
    @classmethod
    def get(cls, cid):
        metabase_session = sessionmaker(bind=m.metabase_engine)()
        try:
            return metabase_session.query(cls).filter_by(id=cid).one()
        except NoResultFound:
            raise Cdb.NotFound(cid) from None

        
    @classmethod
    def all(cls):
        metabase_session = sessionmaker(bind=m.metabase_engine)()
        return list(metabase_session.query(cls))
    
    @orm.reconstructor
    def init_on_load(self):
        pass
    
    @lazy
    def cdb_name(self):
        return "cdb_" + self.id

    @lazy
    def engine(self):
        engine_connect_str = "mssql+pyodbc://{uid}:{pwd}@{srv}/{db}?driver=SQL Server".format(srv=self.server, db=self.cdb_name, uid=aig.uid, pwd=aig.pwd)
        engine = create_engine(engine_connect_str, echo=False, pool_size=60, max_overflow=0)
        return engine

        
    @lazy
    def session(self):
        Session = sessionmaker(bind=self.engine)
        return Session()
        
    def __getitem__(self, lid):
        try:
            lst = self.session.query(Lst).filter_by(id=lid).one()
        except NoResultFound:
            raise Lst.NotFound(self.id, lid) from None
        
        lst.cdb = self
        lst.session = self.session
        lst.engine = self.engine
        return lst

    
    @lazy
    def lists(self):
        lists = set(self.session.query(Lst))
        for l in lists:
            l.cdb = self
            l.session = self.session
        return lists
    
 
    
class Fld(Base):
    __tablename__ = 'fields'

    lid    = Column(String(5), ForeignKey('lists.id'), primary_key=True)
    id       = Column(String(20), primary_key=True)

    name   = Column(String(50))
    oldid  = Column(Integer, nullable=False)

    pvt = Column(Boolean, nullable=False)
    trans = Column(Boolean, nullable=False)
    numeric = Column(Boolean, nullable=False)
    
    vals = relationship('Val', backref='fld', cascade="all, delete, delete-orphan")

    tbl = Column(String(20), nullable=False)
    col = Column(String(20), nullable=False)
    
    size = deferred(Column(Integer))  # only needed where vals are not defined, can remain NULL for others

    join_tbl  = deferred(Column(String(60)))
    join_key  = deferred(Column(String(60)))


    class NotFound(Exception):
        pass

    @lazy
    def valset(self):
        return set(v.val for v in self.vals)
    
    @lazy
    def column(self):
        if not self.size:
            raise Exception("Cannot generate column for field {} because size is not known. Run verify_dbf_field_defs() on list to set field sizes.".format(self))
        if self.numeric:
            coltype = self.get_numtype()
        elif self.vals and {len(v) for v in self.valset} == self.size or self.id in m.MD15_16:
            coltype = CHAR(self.size)
        else:
            coltype = String(self.size)

        is_pkey = False
        is_indexed = False
        if self.pvt:
            is_pkey = True
        elif self.id in m.MD15_16:
            is_indexed = True

        if self.numeric:
            default = '0'
        elif self.id not in m.MD15_16:
            default = ''
        else:
            default = None
        # nullable=False
        return Column(self.col, coltype, primary_key=is_pkey, index=is_indexed, server_default=default)

    

    @lazy
    def session(self):
        return self.lst.session

        
            
class Lst(Base):
    __tablename__ = 'lists'
    IMPORT_TIMESTAMP_RECS = 4000
    
    id    = Column(String(5), primary_key=True)
    name  = Column(String(50))
    oldid = Column(Integer, nullable=False)
    size = deferred(Column(Integer))  # approximate number of records if known
    fields = relationship('Fld', backref='lst')

    def __hash__(self):
        return hash(self.id.lower())

    class NotFound(Exception):
        def __str__(self):
            return "No such list in db: {}".format(self.args)
    
    def __repr__(self):
        return """{}( {}  {}  {} )""".format(self.__class__.__name__, self.id, self.name, self.oldid)
        
    @classmethod
    def get(cls, lid):
        cid = m.SynchLst.get_cid(lid)
        cdb = Cdb.get(cid)
        return cdb[lid]
        
    @lazy
    def schema(self):
        ILLEGAL_LIST_NAMES = {'FOR', 'SET', 'ADD', 'TOP', 'OFF'}
        return self.id if self.id.upper() not in ILLEGAL_LIST_NAMES else self.id + '_'
        
    def __getitem__(self, fid):
        try:
            fld = self.session.query(Fld).filter_by(lid=self.id, id=fid).one()
        except NoResultFound:
            raise Fld.NotFound("Field '{}' not found for {}".format(fid, self))
        return fld

    @lazy
    def engine(self):
        return self.cdb.engine



class Val(Base):
    __tablename__ = 'vals'
    
    lid         = Column(String(5),  primary_key=True)
    fid         = Column(String(20), primary_key=True)
    val         = Column(String(20), primary_key=True)
    name        = Column(String(200))
    __table_args__ = (ForeignKeyConstraint([lid, fid], [Fld.lid, Fld.id]), {})
    
    def __hash__(self):
        return hash((self.lid.upper(), self.fid.lower(), self.val))
    
        
    def __repr__(self):
        return "{}( {}.{} {} {} )".format(self.__class__.__name__, self.lid, self.fid, self.val, self.name)
