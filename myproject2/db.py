import re
import pandas as pd
from stagelib import Folder, from_json, to_json, mkdir, mkpath
#from stagelib.dateutils import Date
#import stagelib.dataframe
from stagelib.db import *

DATADIR = 'data'
database = getdb('adviserinfo', hostalias = 'production')
BaseModel = getbasemodel(database)

def get_filingdate(path):
    return pd.to_datetime(re.sub(r'^ia(\d+)\.zip', r'\1', path))
   
class FormADV(BaseModel):
    filename = CharField()
    filingdate = DateField()

    class Meta:
        datadir = mkdir(DATADIR, 'formadv')
        zipfolder = mkdir(datadir, 'zipfiles')
        db_table = 'formadvs'
        indexes = (
            (('filename', 'filingdate'), True),
                )

    @classmethod
    def mktable(cls):
        __ = Folder.table(cls._meta.zipfolder, pattern = '.zip')
        return __.assign(
            filename = __.basename,
            filingdate = __.basename.map(get_filingdate)
                ).ix[:,['filename', 'filingdate']
                    ].sort_values(by = 'filingdate')
    @classmethod
    def setup(cls):
        for row in cls.mktable().to_dict(orient = 'records'):
            cls.tryinsert(**row)

    @classmethod
    def get_dailyxml(cls):
        pass

class Adviser(BaseModel):
    crd = IntegerField(null = False, constraints = [Check('crd > 0')], index = True)
    secnumber = CharField(max_length = 15)
    name = CharField(max_length = 255, null = False)

    class Meta:
        db_table = 'advisers'

    @property
    def dirname(self):
        return mkdir(settings.DATADIR, self.crd)

    @property
    def brochuredir(self):
        return mkdir(self.dirname, 'brochures')

class AdviserRelation(BaseModel):
    adviser = ForeignKeyField(Adviser, null = False)

class Filing(AdviserRelation):
    formadv = ForeignKeyField(FormADV, null = False, related_name = 'filings')
    assetsundermgmt = FloatField(null = True)
    numberofaccts = FloatField(null = True)
    numberofclients = FloatField(null = True)
    numberofemployees = FloatField(null = True)
    
    class Meta:
        db_table = 'filings'
        order_by = ('formadv', )
        indexes = (
            (('adviser', 'formadv'), True),
                )

class DescriptionText(BaseModel):
    id = PrimaryKeyField(null=False)
    description = CharField(max_length = 255, unique = True)
    specific = BooleanField(default = False)

    class Meta:
        db_table = 'descriptions'

class TypesBase(AdviserRelation):
    formadv = ForeignKeyField(FormADV)
    description = ForeignKeyField(Description, primary_key = True)

class ClientType(TypesBase):
    class Meta:
        db_table = 'clienttypes'
    pass

class ClientTypeAUM(TypesBase):
    class Meta:
        db_table = 'aumbyclienttype'

class Compensation(TypesBase):
    pass

class Disclosures(TypesBase):
    pass #link to court cases ???

class Allegation(AdviserRelation):
    id = PrimaryKeyField(null=False)
    allegation = TextField()

    class Meta:
        db_table = 'allegations'

class CourtCase(AdviserRelation):
    date = DateField(FormADV)
    docketnumber = CharField(max_length = 30)
    district = CharField(max_length = 30)
    allegation = ForeignKeyField(Allegation)
    
    class Meta:
        db_table = 'courtcases'

class OtherBusiness(AdviserRelation):
    name = CharField(max_length = 255, null = False, unique = True)

class PrimeBroker(OtherBusiness):
    class Meta:
        db_table = 'primebrokers'
    pass
    
class Administrator(OtherBusiness):
    class Meta:
        db_table = 'administrators'
    pass

class Custodian(OtherBusiness):
    class Meta:
        db_table = 'administrators'
    pass

class PrivateFund(AdviserRelation):
    fund = IntegerField()
    name = CharField(max_length = 255)
    assetsundermgmt = FloatField()
    primebroker = ForeignKeyField(PrimeBroker, related_name = 'primebrokers')
    administrator = ForeignKeyField(Administrator, related_name = 'administrators')
    custodian = ForeignKeyField(Custodian, related_name = 'custodians')

    class Meta:
        db_table = 'administrators'

database.create_tables([
    FormADV,
    Adviser,
    Filing,
    Description,
    ClientType,
    ClientTypeAUM,
    Compensation,
    Disclosures,
    Allegation,
    CourtCase,
    PrimeBroker,
    Administrator,
    Custodian,
    PrivateFund],
        safe = True)

FormADV.setup()
