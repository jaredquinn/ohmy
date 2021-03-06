""" Provide a handy Object interface to the MySQLdb Module 

Classes:

	MySQLDatabase
	MySQLTable
	MySQLRecord

	MySQLException
	MySQLType



Usage:

	import ohmy

	db = ohmy.MySQLDatabase( host, user, pass, database )
	log = db.table( 'LogHistory' )

	newRecord = log.create()
	newRecord.Id = uuid.uuid4()
	newRecord.UserId = 100
	newRecord.Description = "Generic Click Event"
	newRecord.EventType = 100
	newRecord.Created = datetime.datetime.now()
	newRecord.save()

	all_records = log.select()
	print "There are %d records." % len(records)

	for record in records:
		print " -- Record UserId was %d" % record.UserId

	some_records = log.select(where=['`UserId` = 100'], order=['Created', 'DESC'], limit='1')
	print "There are %d records returned." % len(records)

	

"""

__version__ = '$Revision: $'
__all__ = [ 'MySQLDatabase', 'MySQLTable', 'MySQLRecord', 'MySQLException', 'MysqlType' ]

import logging
LOGGER = logging.getLogger(__name__)

import MySQLdb
import datetime
import dateutil.parser 
import re
import uuid
import enum
import binascii

class MySQLType(object):

	class Representation(enum.Enum):
		" Data Representation; used internally to determine how to encode data "
		INTERNAL = 1
		MYSQL = 2
		EXTERNAL = 3

	class Field(enum.Enum):
		" Basic Field Types that we handle "
		INTEGER = 100
		FLOAT = 101
#		BITS = 102
		DATETIME = 201
#		DATE = 202
#		TIMESTAMP = 203
#		TIME = 203
#		YEAR = 204
		STRING = 301
		BINARY = 302


class MySQLRecord(object):
	" MySQLRecord is the representation of a record in MySQL "

	def __init__(self, table, data = { }, index=None):
		""" Initialise a Record set optionally populating it with 'data'

		table (MySQLTable) - Object Representing the Table associated with this
		                     record.
		data (dict) - Dictionary of Fields and Values to populate the record with.

		"""
		self.__dict__['__DATA'] = dict()
		self.__dict__['__ODATA'] = dict()
		self.__dict__['__INDEX'] = index
		self.__dict__['__PKEY'] = table.getPrimaryKey()
		self.__dict__['__TABLE'] = table
		self.__dict__['__FIELDS'] = table.getFields()
		self.__dict__['__META'] = table.getMeta()


		for i in self.__dict__['__FIELDS']:
			self.__dict__['__DATA'][i] = self.__dict__['__META'][i]['Default']

		for i in data:
			if i in self.__dict__['__FIELDS']: 
					self.setField(i, data[i])

		" Set the record as unchanged "
		self.__sync()


	def __sync(self):
		""" Set the record as unchanged """
		for i in self.__dict__['__FIELDS']:
			self.__dict__['__ODATA'][i] = self.__dict__['__DATA'][i]


	def __getattr__(self, name):
		""" Handle direct getting of fields """
		if name == 'PRIMARY': name = self.__dict__['__PKEY']
		if name in self.__dict__['__FIELDS']:
			return self.getField(name, MySQLType.Representation.EXTERNAL)
		elif self.__dict__.has_key(name):
			return self.__dict__[name]

		
	def __setattr__(self, name, val):
		""" Handle direct setting of fields """
		if name == 'PRIMARY': name = self.__dict__['__PKEY']
		if name in self.__dict__['__FIELDS']:
			return self.setField(name, val)
		else:
			return object.__setattr__(self, name, val)


	def _determineDataRepresentation(self, datarep = None):
		if datarep == None: datarep = MySQLType.Representation.INTERNAL
		if datarep == MySQLType.Representation.INTERNAL or \
		   datarep == MySQLType.Representation.MYSQL or \
		   datarep == MySQLType.Representation.EXTERNAL: return datarep

		raise MySQLException('Unxpected representation %s' % datarep)


	def _externalFieldFormatter( self, field, value ):

		ftype = self.__dict__['__META'][field]['DataType'] 
		if value == None: return 'NULL'

		if ftype == MySQLType.Field.BINARY:		return binascii.hexlify(value)
		if ftype == MySQLType.Field.DATETIME: 	return value
		if ftype == MySQLType.Field.STRING:     return str(value)
		if ftype == MySQLType.Field.INTEGER:   	return int(value)
		if ftype == MySQLType.Field.FLOAT:   	return float(value)

		raise TypeError('Unknown Field Type %s' % META[field]['Type'])

	def _mysqlFieldFormatter( self, field, value ):

		ftype = self.__dict__['__META'][field]['DataType'] 
		if value == None: return 'NULL'

		if ftype == MySQLType.Field.BINARY:	return "x'%s'" % binascii.hexlify(value)
		if ftype == MySQLType.Field.DATETIME: 	return "'%s'" % value.strftime('%Y-%m-%d %H:%M:%S')
		if ftype == MySQLType.Field.STRING:     return "'%s'" % value.replace("'", "\\\'")
		if ftype == MySQLType.Field.INTEGER:   	return int(value)
		if ftype == MySQLType.Field.FLOAT:   	return "%f" % value

		raise TypeError('Unknown Field Type %s' % self.__dict__['__META'][field]['Type'])


	def getField(self, field, datarep=None, value=None):
		""" Base getField Interface allowing any required data transforms

		    field (str) - Name of Field to Retrieve
		"""
		if field == 'PRIMARY': field = self.__dict__['__PKEY']
		datarep = self._determineDataRepresentation(datarep)
		if field in self.__dict__['__FIELDS']:
			val = self.__dict__['__DATA'][field] 
			if value != None:
				val = value
			if datarep == MySQLType.Representation.MYSQL: val = self._mysqlFieldFormatter( field, val )
			if datarep == MySQLType.Representation.EXTERNAL: val = self._externalFieldFormatter( field, val )
			return val
		else:
			raise MySQLException('Field %s is not defined' % field)


	def setField(self, field, value):
		""" Base setField Interface allowing any required data transforms

		    field (str) - Name of the Field to Set
			value (arbitary) - Value to set the field to
		"""
		if field == 'PRIMARY': field = self.__dict__['__PKEY']
		if not self.__dict__.has_key('__ODATA'):
			""" Before we set any values for the first time
			    we keep a copy of the data as loaded
			"""
			self.__dict__['__ODATA'] = self.__dict__['__DATA']

		ftype = self.__dict__['__META'][field]['DataType']

		if ftype == MySQLType.Field.DATETIME and value != None:
			if not isinstance(value, datetime.datetime):
				value = dateutil.parser.parse(value)

		if ftype == MySQLType.Field.BINARY and value != None:
			if not isinstance(value, bytes):
				if isinstance(value, str): value = uuid.UUID(value).bytes
				elif isinstance(value, int): value = bin(value)
				elif isinstance(value, uuid.UUID): value = value.bytes

		if field in self.__dict__['__FIELDS']:
			""" Validation of Data should occur here """
			self.__dict__['__DATA'][field] = value
			return self.__dict__['__DATA'][field]

		else:
			raise AttributeException('Field %s is not defined' % field)


	def isModified(self):
		""" Has the record been modified since load or last save?
		"""
		for i in self.__dict__['__FIELDS']:
			if self.__dict__['__DATA'][i] != self.__dict__['__ODATA'][i]:
				return True
		return False


	def changes(self, datarep=None):
		""" Return the changeset for this record
		"""
		res = { }
		for i in self.__dict__['__FIELDS']:
			if self.__dict__['__DATA'][i] != self.__dict__['__ODATA'][i]:
				res[i] = self.getField(i, datarep)
		return res


	def data(self, datarep=None):
		""" Return all fields in the current object model.
		"""
		res = {}
		for i in self.__dict__['__FIELDS']: 
			res[i] = self.getField(i, datarep)

		return res


	def save(self):
		""" Write the record back to the database
		"""
		if self.__dict__['__INDEX'] == None:
			return self.__dict__['__TABLE'].insert(self)
		else:
			return self.__dict__['__TABLE'].save(self)


class MySQLRecordSet(list):

	def __setitem__(self, key, value):
		if not isinstance(value, MySQLRecord):
			raise TypeError('%s is not a MySQLRecord', item)
		list.__setitem__(self, key, value)


class MySQLTable(object):
		
	def __init__(self, database, tablename):
		self.__META = {} 
		self.__FIELDS = []
		self.__TABLENAME = tablename
		self.__db = database
		self.__PKEY = None

		if self.__db == None:
			raise MySQLException('No Database Connection')

		self.__db.checkConnection()
		cur = self.__db.connection().cursor()
		cur.execute('DESCRIBE `%s`' % tablename)
		res = cur.fetchall()
		for row in res:
			if row[3] == 'PRI': self.__PKEY = row[0]
			self.__FIELDS.append(row[0])
			self.__META[row[0]] = {
				'Type': row[1],
				'DataType': self._getInternalFieldType( row[1] ),
				'Null': row[2],
				'Key': row[3],
				'Default': row[4],
				'Extra': row[5]
			}
		cur.close()

	def _getInternalFieldType( self, ftype ):
		if re.search('^binary', ftype) != None: return MySQLType.Field.BINARY
		if re.search('^datetime', ftype) != None: return MySQLType.Field.DATETIME
		if re.search('^(smallint|int|tinyint|mediumint|bigint)', ftype) != None: return MySQLType.Field.INTEGER
		if re.search('^(number|decimal|float|double)', ftype) != None: return MySQLType.Field.FLOAT
		if re.search('^(text|char|blob|varchar|string)', ftype) != None: return MySQLType.Field.STRING
		return 0


	def _fieldString(self, keys):
		return ','.join( map(lambda v: '`%s`' % v, keys ) )


	def _dataString(self, data):
		return ','.join( map(lambda v: '%s' % data[v], data.keys() ) )

	def _setString(self, data= None):
		return ",".join( map(lambda v: '`%s` = %s' % ( v, data[v] ), data.keys() ) )

	def _joinString(self, string):
		joinString = ''
		if string != None:
			joinString = string	
		return string

	def _whereString(self, data=None):
		whereStr = ""

		if isinstance(data, list) or isinstance(data, tuple):
			whereStr = 'WHERE %s' % ",".join(data)
		elif isinstance(data, str):
			whereStr = 'WHERE %s' % data
		elif data == None:
			whereStr = ''
		else:
			raise MySQLException('What do I do with %s' % str(type(data)))

		return whereStr


	def _orderString(self, data):
		field = None

		if data:
			field = data[0] if isinstance(data, list) else str(data)
			direct = data[1] if isinstance(data, list) else 'ASC'

			if field in self.__FIELDS:
				return 'ORDER BY `%s` %s' % ( field, direct )
			else:
				raise AttributeError('Unknown Field %s' % field )

		return ""


	def _groupString(self, data = None):
		if data:
			if data in self.__FIELDS:
				return 'GROUP BY `%s`' % data
			else:
				raise AttributeError('Unknown Field %s' % data)
		return ""


	def _limitString(self, data = None):
		if data != None:
			if type(data) == 'list': return 'LIMIT %d, %d' % ( data[0], data[1] )
			return 'LIMIT %d' % data
		return ""

	def getMeta(self):
		return self.__META

	def getFields(self):
		return self.__FIELDS

	def getPrimaryKey(self):
		return self.__PKEY

	def create(self, data = { }):
		record = MySQLRecord(self, data, None)
		return record

	def _mapColumnsToKeys(self, fields, row):
		count = 0
		res = dict()
		for i in row:
			res[ fields[count] ] = row[count]
			count = count + 1
		return res


	def _mapResultToRecordSet(self, fields, resultSet):
		ret = MySQLRecordSet()
		for row in resultSet:
			rowDict = self._mapColumnsToKeys( fields, row )
			recObj  = MySQLRecord( self, rowDict, rowDict[ self.__PKEY ] )
			ret.append( recObj )
		return ret
			

	def _fetchall(self, cur):
		v = cur.fetchall()
		cur.close()
		return v

	def _execute(self, statement, insert=False):
		self.__db.checkConnection()
		cur = self.__db.connection().cursor()
		LOGGER.debug('Executing %s' % statement)
		res = cur.execute(statement)

		if insert:
			pkey = cur.lastrowid
			self.__INDEX = pkey
			LOGGER.debug('Setting Transaction ID Field %s=%s' % ( self.__PKEY, pkey ))

		return cur

	def _check_result(self, cur):
		if cur.rowcount != 1:
			raise MySQLException('Expected to Update only 1 row but updated %s' % cur.rowcount)

	
	def get(self, pkey, key=None, createIfNone=False, failIfNone=True):
		" Convenience method to return a MySQLRecord matching the PKEY value "
		if not key: key=self.__PKEY

		tr=self.create()
		pkey=tr.getField( key, datarep=MySQLType.Representation.MYSQL, value=pkey )
		
		res = self.select( where=["`%s` = %s" % ( key, pkey ) ])

		if len(res) == 1:
			return res[0]

		if createIfNone:
			return self.create()

		if failIfNone:
			raise MySQLException('ID Mismatch')

		return None


	def save(self, record):
		" Convenience method to run an update again the changed fields of the current record "
		values = record.changes( MySQLType.Representation.MYSQL )
		kf = record.getField( self.__PKEY, datarep=MySQLType.Representation.MYSQL, value=None )
		up = self.update( values, conditions=[ "`%s`=%s" % ( self.__PKEY, kf ) ] )
		return up


	def select(self, fields=None, where=None, order=None, group=None, join=None, limit=None ):
		if fields == None: fields = self.__FIELDS

		statement = 'SELECT %s FROM `%s` %s %s %s %s %s;' % ( 
				self._fieldString( fields ),
				self.__TABLENAME, 
				self._joinString( join ),
				self._whereString( where ),
				self._orderString( order ),
				self._groupString( group ),
				self._limitString( limit )
		)

		res = self._fetchall( self._execute(statement) )
		val = self._mapResultToRecordSet(fields, res)
		return val



	def insert(self, data=None):
		LOGGER.debug('Is INSERT')

		if isinstance(data, dict): record = self.create(dict)
		if isinstance(data, MySQLRecord): record = data
		
		data = record.data( MySQLType.Representation.MYSQL )

		statement = 'INSERT INTO `%s` (%s) VALUES (%s);' %  ( 
				self.__TABLENAME, 
				self._fieldString( data.keys() ), 
				self._dataString( data ) 
		)

		LOGGER.debug('Statement %s' % statement)
		cur = self._execute( statement, insert=True )
		self._check_result( cur )
		cur.close()

		record.setField( self.__PKEY, self.__INDEX )
		rec = self.get( record.getField( self.__PKEY ) )
		return rec

	def update(self, values, conditions):

		statement = 'UPDATE `%s` SET %s %s' % ( 
				self.__TABLENAME, 
				self._setString( values ), 
				self._whereString( conditions )
		)
		LOGGER.debug('Statement %s' % statement)
		cur = self._execute( statement )
		#self._check_result( cur )

		return True



class MySQLException(MySQLdb.Error):
	pass

class MySQLDatabase(object):
	" MySQLDatabase Object handing database connection "

	def __init__(self, host, username, password, database):
		""" 
		   host (str) - hostname of MySQL server
		   username (str) - username to connect to the server as
		   password (str) - password to connect to the server with
		   database (str) - database to use
		"""
		self.__username = username
		self.__password = password
		self.__host = host
		self.__database = database
		self.__status = 0

		self.__conn = None

	def checkConnection(self):
		if self.__conn == None:  self.connect()
		cur = self.connection().cursor()
		try:
			cur.execute('SELECT 1')
			self.__status = True
		except MySQLdb.OperationalError as e:
			if e[0] == 2006:
				self.__status = False
				self.__conn = None
				self.connect()
			else:
				raise
		
	def connect(self):
		self.__conn = MySQLdb.connect(self.__host, self.__username, self.__password, self.__database, charset='utf8', use_unicode=True)
		
		self.__conn.autocommit(True)

		if self.__conn == None:
			raise MySQLException('connect() failed')

		return self.__conn

	def connection(self):
		if self.__conn == None:  self.connect()
		return self.__conn

	def table(self, tablename):
		if self.__conn == None:  self.connect()
		return MySQLTable(self, tablename)


