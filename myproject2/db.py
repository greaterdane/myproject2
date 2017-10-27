from stagelib.db import *
from stagelib import OSPath
from stagelib import mkdir as newfolder

database = getdb('adviserinfo', hostalias = 'production')

def setup():
    database.create_tables([
        FormADV,
        Adviser,
        AlternateName,
        JobTitle,
        Person,
        Phone,
        Fax,
        Address,
        Website,
        Filing,
        SecFiler,
        Description,
        ClientType,
        ClientTypeAUM,
        Compensation,
        Disclosure,
        Courtcase,
        Allegation,
        PrivateFund,
        OtherBusiness,
        BusinessRelation,
        FundBackOffice],
            safe = True)

BaseModel = get_basemodel(database)

class AdvBaseModel(BaseModel):
    @classmethod
    def insertdf(cls, df, extrafields = ['filing', 'adviser', 'formadv'], **kwds):
        return super(AdvBaseModel, cls).insertdf(df, extrafields = extrafields, **kwds)
    
class FormADV(AdvBaseModel):
    date = DateField()
    filename = CharField()

    class Meta:
        db_table = 'formadvs'
        indexes = (
            (('date', 'filename'), True),
                )

    @classmethod
    def datesdict(cls):
        return cls.getdict('date')

    def __repr__(self):
        return self.date.strftime("FormADV: {}/%Y-%m-%d".format(OSPath.basename(self.filename)))

class Adviser(AdvBaseModel):
    crd = IntegerField(null = False, constraints = [Check('crd > 0')], index = True, primary_key = True)
    secnumber = CharField(max_length = 15)
    name = CharField(max_length = 255, null = False)
    legalname = CharField(max_length = 255)
    registered = BooleanField(default = True)
    #namechangedate = DateField()

    class Meta:
        db_table = 'advisers'

    @classmethod
    def insertdf(cls, df, extrafields = ['crd'], **kwds):
        fromdb = cls.to_dataframe(cls.select(cls.crd, cls.secnumber, cls.name))
        if not fromdb.empty:
            namechanges = df.loc[
                (df.crd.isin(fromdb.crd)) & ~(df.name.replace(',', '').isin(fromdb.name.replace(',', '')))
                    ].ix[:, ['crd', 'secnumber', 'name', 'legalname']]
    
            oldnames = fromdb.loc[
                fromdb.crd.isin(namechanges.crd)
                    ].rename(columns = {'crd' : 'adviser'})
            
            for change in namechanges.to_dict(orient = 'records'):
                cls.update(**change).where(cls.crd == change['crd']).execute()
    
            AlternateName.insertdf(oldnames)
            df = df.loc[~df.crd.isin(namechanges.crd)]

        return super(Adviser, cls).insertdf(df,
            extrafields = extrafields, **kwds)

    @property
    def dirname(self):
        return newfolder('data', self.crd)

    @property
    def brochuredir(self):
        return newfolder(self.dirname, 'brochures')

    def __repr__(self):
        return "{} (CRD# {})".format(self.name, self.crd)

class Filing(AdvBaseModel):
    adviser = ForeignKeyField(Adviser, related_name = "filings")
    formadv = ForeignKeyField(FormADV, related_name = "all_filings")
    
    class Meta:
        db_table = 'filings'
        order_by = ('formadv', )
        indexes = (
            (('adviser', 'formadv'), True),
                )
    
    @classmethod
    def getdict(cls, formadv):
        rows = cls.to_records(cls.select()
            .where(cls.formadv == formadv.id))
        return {row['adviser'] : row['id'] for row in rows}

class SecFiler(AdvBaseModel):
    adviser = ForeignKeyField(Adviser, primary_key = True, related_name = 'secfilers')
    cik = IntegerField()

    class Meta:
        db_table = 'secfilers'

class FilingBaseModel(AdvBaseModel):
    filing = ForeignKeyField(Filing, primary_key = True)

    class Meta:
        order_by = ('filing', )
    
    @classmethod
    def insertdf(cls, df, extrafields = ['filing'], **kwds):
        return super(FilingBaseModel, cls).insertdf(df,
            extrafields = extrafields, **kwds)
        

class JobTitle(AdvBaseModel):
    name = CharField(max_length = 120, unique = True)

    class Meta:
        db_table = 'jobtitles'

class Person(AdvBaseModel):
    adviser = ForeignKeyField(Adviser, related_name = 'people')
    title = CharField(max_length = 30)
    firstname = CharField(max_length = 50, null = False)
    lastname = CharField(max_length = 50, null = False)
    phone = CharField(max_length = 30)
    date = DateField()

    class Meta:
        db_table = 'people'
        indexes = (
            (('adviser', 'title', 'firstname', 'lastname'), True),
                )

    @classmethod
    def insertdf(cls, df, extrafields = ['adviser'], **kwds):
        if not hasattr(df, 'firstname'):
            db_logger.warning("'Person' data has no 'firstname' attribute.  0 rows inserted.")
            return 0
        return super(Person, cls).insertdf(df,
            extrafields = extrafields, **kwds)
        
class AlternateName(AdvBaseModel):  #to handle name changes
    adviser = ForeignKeyField(Adviser, related_name = 'alternate_names')
    secnumber = CharField(max_length = 15)
    name = CharField(max_length = 255, null = False)

    class Meta:
        db_table = 'alternate_names'
        indexes = (
            (('name', 'secnumber'), True),
                )

class Phone(FilingBaseModel):
    phone = CharField(max_length = 30)

    class Meta:
        indexes = (
            (('phone',), True),
                )

class Fax(FilingBaseModel):
    fax = CharField(max_length = 30)
    class Meta:
        indexes = (
            (('fax',), True),
                )

class Address(FilingBaseModel):
    fulladdress = CharField(null = False)
    address1 = CharField(max_length = 175)
    address2 = CharField(max_length = 50)
    city = CharField(max_length = 50)
    state = CharField(max_length = 2)
    zip = CharField(max_length = 15)
    country = CharField(max_length = 25)
    
    class Meta:
        indexes = (
            (('address1', 'address2', 'city', 'state', 'zip', 'country', ), True),
                )

class Website(FilingBaseModel):
    url = CharField(max_length = 255)
    class Meta:
        indexes = (
            (('url', ), True),
                )

class Numbers(FilingBaseModel):
    assetsundermgmt = FloatField(null = True)
    numberofaccts = FloatField(null = True)
    numberofclients = FloatField(null = True)
    numberofemployees = FloatField(null = True)

class Description(BaseModel): ##HERE
    id = PrimaryKeyField(null=False)
    text = CharField(max_length = 255, unique = True)
    specific = BooleanField(default = False)

    class Meta:
        db_table = 'descriptions'

    @classmethod
    def textdict(cls):
        return cls.getdict('text', reversed = True)

class ClientType(FilingBaseModel):
    description = ForeignKeyField(Description, related_name = 'client_types')
    percentage = FloatField(null = False)

    class Meta:
        db_table = 'client_types'

class ClientTypeAUM(FilingBaseModel):
    description = ForeignKeyField(Description, related_name = 'client_types_aum')
    percentage = FloatField(null = False)

    class Meta:
        db_table = 'pct_aum'

class Compensation(FilingBaseModel):
    description = ForeignKeyField(Description, related_name = 'compensation')
    percentage = FloatField(null = False)

class Disclosure(FilingBaseModel):
    description = ForeignKeyField(Description, related_name = 'disclosures')
    number = IntegerField(null = False)

    class Meta:
        db_table = 'disclosures'

class Courtcase(AdvBaseModel):
    id = PrimaryKeyField(null=False)
    adviser = ForeignKeyField(Adviser, null = False, related_name = 'courtcases')
    number = CharField(max_length = 30, help_text = "Docket or case number.")
    district = CharField(max_length = 30)
    resolution = CharField()
    renderedfine = FloatField()
    sanctions = CharField()
    date = DateField()

    class Meta:
        db_table = 'courtcases'

class Allegation(AdvBaseModel):
    case = ForeignKeyField(Courtcase, primary_key = True)
    allegation = TextField()

    class Meta:
        db_table = 'allegations'

class PrivateFund(AdvBaseModel):
    adviser = ForeignKeyField(Adviser)
    fund_id = IntegerField()
    name = CharField(max_length = 255)
    type = CharField(max_length = 100)
    assetsundermgmt = FloatField()
    region = CharField()
    dated = DateField()

    class Meta:
        db_table = 'privatefunds'

class OtherBusiness(AdvBaseModel):
    name = CharField(max_length = 255, null = False)
    type = CharField(max_length = 100, null = False)
    
    class Meta:
        indexes = (
            (('name', 'type'), True),
                )

class BusinessRelation(AdvBaseModel):
    business = ForeignKeyField(OtherBusiness)
    adviser = ForeignKeyField(Adviser)

class FundBackOffice(AdvBaseModel):
    id = PrimaryKeyField()
    business = ForeignKeyField(OtherBusiness)
    privatefund = ForeignKeyField(PrivateFund)
    
setup()
