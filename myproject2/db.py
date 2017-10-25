import os
from stagelib.web import HomeBrowser
from stagelib.db import *

from adviserinfo2 import *

database = getdb('adviserinfo', hostalias = 'production')
BaseModel = getbasemodel(database)

def normalize_formadvs():
    parser = FormADVStage()
    formadvs = FormADV.select()
    for formadv in formadvs:
        formadv.data

def setup():
    database.create_tables([
        FormADV,
        Adviser,
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

    FormADV.download()
    filelist = FormADV.listforms()
    FormADV.tryinsert(filelist)

class FormADV(BaseModel):
    date = DateField()
    filename = CharField()

    def __repr__(self):
        return self.date.strftime("FormADV: {}/%Y-%m-%d".format(OSPath.basename(self.filename)))

    class Meta:
        db_table = 'formadvs'
        indexes = (
            (('date', 'filename'), True),
                )

    @classmethod
    def getdates(cls):
        return {row.id : row.date for row in cls.select()}

    @classmethod
    def listforms(cls):
        return sorted([
            {'date' : get_filingdate(path), 'filename' : path}
            for path in Folder.listdir(zipfolder)
                ], key = lambda k: k['date'])

    @classmethod
    def download(cls):
        br = HomeBrowser(starturl = r'https://www.sec.gov/help/foiadocsinvafoiahtm.html')
        for linktag in br.filterlinks(r'\d{6}\.zip'):
            url = linktag.url
            link = "https://www.sec.gov/%s" % url
            _ = os.path.split(link)[-1]
            br.download(link, outfile = mkpath(cls._meta.zipfolder, _))

    @classmethod
    def get_dailyxml(cls):
        pass

    @property
    def data(self):
        return read_formadv(self.filename)

class Adviser(BaseModel):
    crd = IntegerField(null = False, constraints = [Check('crd > 0')], index = True, primary_key = True)
    secnumber = CharField(max_length = 15)
    name = CharField(max_length = 255, null = False)
    legalname = CharField(max_length = 255)
    registered = BooleanField(default = True)
    
    def __repr__(self):
        return "{} (CRD# {})".format(self.name, self.crd)

    class Meta:
        db_table = 'advisers'

    @property
    def dirname(self):
        return to_datafolder(self.crd)

    @property
    def brochuredir(self):
        return newfolder(self.dirname, 'brochures')

class JobTitle(BaseModel):
    name = CharField(max_length = 120, unique = True)

    class Meta:
        db_table = 'jobtitles'

class Person(BaseModel):
    formadv = ForeignKeyField(FormADV, related_name = 'all_people')
    adviser = ForeignKeyField(Adviser, related_name = 'people')
    title = ForeignKeyField(JobTitle, related_name = 'names')
    firstname = CharField(max_length = 50)
    lastname = CharField(max_length = 50)

    class Meta:
        db_table = 'people'
        indexes = (
            (('adviser', 'title', 'firstname', 'lastname'), True),
                )

class ContactNumber(BaseModel):
    number = CharField(max_length = 30)

    class Meta:
        indexes = (
            (('adviser', 'number'), True),
                )

class Phone(ContactNumber):
    formadv = ForeignKeyField(FormADV, related_name = 'all_phonenumbers')
    adviser = ForeignKeyField(Adviser, related_name = 'phonenumbers')

class Fax(ContactNumber):
    formadv = ForeignKeyField(FormADV, related_name = 'all_faxnumbers')
    adviser = ForeignKeyField(Adviser, related_name = 'faxnumbers')

class Address(BaseModel):
    formadv = ForeignKeyField(FormADV, related_name = 'all_addresses')
    adviser = ForeignKeyField(Adviser, related_name = 'addresses')
    fulladdress = CharField(null = False)
    address1 = CharField(max_length = 175)
    address2 = CharField(max_length = 50)
    city = CharField(max_length = 50)
    state = CharField(max_length = 2)
    zip = CharField(max_length = 15)
    country = CharField(max_length = 25)

    class Meta:
        indexes = (
            (('adviser', 'fulladdress'), True),
                )

class Website(BaseModel):
    adviser = ForeignKeyField(Adviser, related_name = 'websites')
    url = CharField(max_length = 255)

    class Meta:
        db_table = 'websites'
        indexes = (
            (('adviser', 'url'), True),
                )

class SecFiler(BaseModel):
    adviser = ForeignKeyField(Adviser, primary_key = True)
    cik = IntegerField()

class Filing(BaseModel):
    adviser = ForeignKeyField(Adviser, null = False, related_name = 'filings')
    formadv = ForeignKeyField(FormADV, null = False, related_name = 'filing_data')
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

class Description(BaseModel):
    id = PrimaryKeyField(null=False)
    text = CharField(max_length = 255, unique = True)
    specific = BooleanField(default = False)

    class Meta:
        db_table = 'descriptions'

class ClientType(BaseModel):
    adviser = ForeignKeyField(Adviser, null = False, related_name = 'client_types')
    description = ForeignKeyField(Description, related_name = 'client_types')
    percentage = FloatField(null = False)

    class Meta:
        db_table = 'client_types'

class ClientTypeAUM(BaseModel):
    adviser = ForeignKeyField(Adviser, null = False, related_name = 'client_types_aum')
    description = ForeignKeyField(Description, related_name = 'client_types_aum')
    percentage = FloatField(null = False)

    class Meta:
        db_table = 'client_types_aum'

class Compensation(BaseModel):
    adviser = ForeignKeyField(Adviser, null = False, related_name = 'compensated_by')
    description = ForeignKeyField(Description, related_name = 'compensation')
    percentage = FloatField(null = False)

class Disclosure(BaseModel):
    adviser = ForeignKeyField(Adviser, null = False, related_name = 'disclosures')
    description = ForeignKeyField(Description, related_name = 'disclosures')
    count = IntegerField(null = False)

class Courtcase(BaseModel):
    id = PrimaryKeyField(null=False)
    adviser = ForeignKeyField(Adviser, null = False, related_name = 'courtcases')
    date = DateField()
    number = CharField(max_length = 30, help_text = "Docket or case number.")
    district = CharField(max_length = 30)
    
    class Meta:
        db_table = 'courtcases'

class Allegation(BaseModel):
    case = ForeignKeyField(Courtcase, primary_key = True)
    allegation = TextField()

    class Meta:
        db_table = 'allegations'

class PrivateFund(BaseModel):
    adviser = ForeignKeyField(Adviser)
    fund = IntegerField()
    name = CharField(max_length = 255)
    assetsundermgmt = FloatField()

    class Meta:
        db_table = 'privatefunds'

class OtherBusiness(BaseModel):
    name = CharField(max_length = 255, null = False)
    type = CharField(max_length = 100, null = False)
    
    class Meta:
        indexes = (
            (('name', 'type'), True),
                )

class BusinessRelation(BaseModel):
    business = ForeignKeyField(OtherBusiness)
    adviser = ForeignKeyField(Adviser)

class FundBackOffice(BaseModel):
    business = ForeignKeyField(OtherBusiness)
    privatefund = ForeignKeyField(PrivateFund)
