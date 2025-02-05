import re
from functools import partial
import pandas as pd
from stagelib.db import *
from stagelib import ospath, Folder, mergedicts, floating_point, readjson, newfolder, joinpath
from stagelib.record import getname

re_PERCENTAGE = re.compile(r'^(?:[^\d]+)?(\d+)[^\d]+.*?$')
re_ALLEGATION = re.compile(r'(^.*?)(?:\s+Sanctions:.*?$|$)')
re_DOLLARAMT = re.compile(r'(\$[\d,\.]+)')
re_BUSINESSINFO = re.compile(r'(^.*?)(?:\s+CRD.*?$|\s+(?:Not\s+)?Registered.*?$|$)')
re_NOTBUSINESS = re.compile(r'Public Office|Private Residence')

formadv_folder = newfolder('data', 'formadv')
advfolder = partial(newfolder, formadv_folder)
zipfolder = advfolder('zipfiles')
unzippedfolder = advfolder('unzipped')
xmlfolder = advfolder('dailyxml')

preprocessed = advfolder('preprocessed')

database = getdb('adviserinfo', hostalias = 'production')

def setup():
    database.create_tables([
        FormADV,
        Adviser,
        AlternateName,
        Person,
        Ownership,
        Phone,
        Fax,
        Address,
        Website,
        Filing,
        SecFiler,
        Numbers,
        Description,
        ClientType,
        ClientTypeAUM,
        Compensation,
        Disclosure,
        Courtcase,
        Allegation,
        PrivateFund,
        OtherBusiness,
        AdviserRelation,
        FundRelation],
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

    @property
    def unzippedfolder(self):
        return newfolder(unzippedfolder, self.date.strftime("%Y%m%d"))

    @property
    def unzippedfile(self):
        for filename in Folder.listdir(self.unzippedfolder):
            return filename

    @property
    def outfile(self):
        return joinpath(preprocessed,
            self.date.strftime("%m%d%y_output.csv"))

    @classmethod
    def datesdict(cls):
        return cls.getdict('date')

    def __repr__(self):
        return self.date.strftime("FormADV: {}/%Y-%m-%d".format(ospath.basename(self.filename)))

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

    @property
    def scheduleD(self):
        return from_json(mkpath(self.dirname, 'predictiveops.json'))

    @property
    def privatefunds(self):
        return self.scheduleD['funds']

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
    def mostrecent(cls):
        alias = cls.alias()
        subquery = (alias.select( #Subquery to get max filing per adviser.
            alias.adviser,
            fn.MAX(alias.id).alias('most_recent'))
            .group_by(alias.adviser)
            .alias('most_recent_subquery'))

        mostrecent = (
            cls.select(cls.id.alias("filing_id"), Adviser.crd)
            .join(Adviser)
            .switch(cls)
            .join(subquery, on=(
                (cls.id == subquery.c.most_recent) &
                (cls.adviser == subquery.c.adviser_id)))
                    )

        return cls.to_dataframe(mostrecent)

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
        indexes = (
            (('adviser', 'cik', ), True),
                )

class FilingBaseModel(AdvBaseModel):
    filing = ForeignKeyField(Filing, primary_key = True)

    class Meta:
        order_by = ('filing', )

    @classmethod
    def insertdf(cls, df, extrafields = ['filing'], **kwds):
        return super(FilingBaseModel, cls).insertdf(df,
            extrafields = extrafields, **kwds)

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

    @classmethod
    def addpeople(cls, crd, people):
        with database.atomic():
            for person in people:
                row = mergedicts(getname(person['name']), adviser = crd)
                while True:
                    try:
                        entry = cls.get(**row); break
                    except cls.DoesNotExist:
                        cls.insert(**row).execute()

                try:
                    percentage = re_PERCENTAGE.sub(r'.\1', person['ownership'])
                except KeyError:
                    percentage = '0'

                is_cntrlperson = person['controlperson']
                try:
                    Ownership.get_or_create(person = entry,
                        percentowned = percentage,
                        controlperson = is_cntrlperson)
                except IntegrityError as e:
                    db_logger.error(e)

                try:
                    title = ' '.join(x.capitalize() for x in person['title'].split())
                except KeyError:
                    title = 'n/a'

                try:
                    title = ' '.join(x.capitalize() for x in person['title'].split())
                except KeyError:
                    title = 'n/a'

                try:
                    since = pd.to_datetime(person['since'])
                except (KeyError, Exception) as e:
                    db_logger.error(e)
                    since = None

                (cls.update(date = since, title = title)
                    .where((cls.firstname == entry.firstname) &
                        (cls.lastname == entry.lastname) &
                        (cls.adviser == crd)).execute())

class Ownership(BaseModel):
    person = ForeignKeyField(Person, primary_key = True)
    controlperson = BooleanField(default = True)
    percentowned = FloatField()

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

class Description(BaseModel):
    id = PrimaryKeyField(null=False)
    text = CharField(max_length = 255, unique = True)
    specific = BooleanField(default = False)

    class Meta:
        db_table = 'descriptions'

    @classmethod
    def textdict(cls):
        return cls.getdict('text', reversed = True)

class ClientType(AdvBaseModel):
    description = ForeignKeyField(Description, related_name = 'client_types')
    filing = ForeignKeyField(Filing, related_name = 'client_types')
    percentage = FloatField(null = False)

    class Meta:
        db_table = 'client_types'

class ClientTypeAUM(AdvBaseModel):
    description = ForeignKeyField(Description, related_name = 'client_types_aum')
    filing = ForeignKeyField(Filing, related_name = 'client_types_aum')
    percentage = FloatField(null = False)

    class Meta:
        db_table = 'pct_aum'

class Compensation(AdvBaseModel):
    description = ForeignKeyField(Description, related_name = 'compensation')
    filing = ForeignKeyField(Filing, related_name = 'compensation')
    percentage = FloatField(null = False)

class Disclosure(AdvBaseModel):
    description = ForeignKeyField(Description, related_name = 'disclosures')
    filing = ForeignKeyField(Filing, related_name = 'disclosures')
    number = IntegerField(null = False)

    class Meta:
        db_table = 'disclosures'

class Courtcase(AdvBaseModel):
    id = PrimaryKeyField(null=False)
    adviser = ForeignKeyField(Adviser, null = False, related_name = 'courtcases')
    number = CharField(max_length = 30, help_text = "Docket or case number.", null = True)
    district = CharField(max_length = 30, null = True)
    resolution = CharField(null = True)
    renderedfine = FloatField(null = True)
    sanctions = CharField(null = True)
    date = DateField()

    class Meta:
        db_table = 'courtcases'

    @classmethod
    def addcases(cls, crd, disclosures):
        for case in disclosures:
            fine = case['renderedfine']
            if not fine:
                fine = case['amendedfine']
                if not fine:
                    _search = re_DOLLARAMT.search(str(case['sanctions']))
                    if _search:
                        fine = _search.group(1)
            row = mergedicts({
                k : (floating_point(fine) if k == 'renderedfine'
                    else pd.to_datetime(v) if k == 'date' else v)
                    for k, v in case.items() if k in cls._meta.fields
                        }, adviser = crd)

            entry = cls.get_or_create(**row)[0]
            if case['allegation']:
                try:
                    Allegation.get_or_create(case = entry.id,
                        allegation = re_ALLEGATION.sub(r'\1', case['allegation']))
                except IntegrityError:
                    pass

class Allegation(BaseModel):
    case = ForeignKeyField(Courtcase, primary_key = True)
    allegation = TextField()

    class Meta:
        db_table = 'allegations'

class PrivateFund(AdvBaseModel):
    adviser = ForeignKeyField(Adviser)
    fund_id = CharField(max_length = 20)
    name = CharField(max_length = 255)
    type = CharField(max_length = 100)
    assetsundermgmt = FloatField()
    region = CharField()
    dated = DateField()

    class Meta:
        db_table = 'privatefunds'
        indexes = (
            (('adviser', 'fund_id', 'dated'), True),
                )

    @classmethod
    def addfund(cls, crd, fund):
        info = fund['fundinfo']
        _ = fund['fund_id']
        aum = info['assetsundermgmt']
        if not aum:
            aum = 0

        row = mergedicts({
            k : (v if k != 'assetsundermgmt' else floating_point(aum))
                for k, v in info.items() if k in cls._meta.fields
                    }, type = info['fundtype'], fund_id = "{}-{}".format(_[0:3], _[3:]),
                        adviser_id = crd, name = fund['name'])

        try:
            entry = cls.get_or_create(**row)
        except IntegrityError as e:
            db_logger.error(e)
            return cls.get(adviser = crd, fund_id = row['fund_id'])
        return entry[0]

class OtherBusiness(BaseModel):
    name = CharField(max_length = 255, null = False)
    type = CharField(max_length = 150, null = False)
    info = CharField(max_length = 350, null = True)

    class Meta:
        db_table = 'other_businesses'
        indexes = (
            (('name', 'type', 'info'), True),
                )

    @classmethod
    def create_relationship(cls, row, businessrow):
        if re_NOTBUSINESS.search(businessrow['name']):
            return

        info = businessrow['info']
        businessrow['info'] = re_BUSINESSINFO.search(info).group(1).upper()
        entry, created = cls.get_or_create(**businessrow)
        return mergedicts(row, business = entry.id)

class AdviserRelation(AdvBaseModel):
    business = ForeignKeyField(OtherBusiness)
    adviser = ForeignKeyField(Adviser)

    class Meta:
        db_table = 'adviser_relationships'
        indexes = (
            (('business', 'adviser'), True),
                )

    @classmethod
    def create_relationships(cls, crd, businesses):
        relationships = []
        for businessrow in businesses:
            relationship = OtherBusiness.create_relationship({'adviser' : crd}, businessrow)
            if not relationship:
                continue
            relationships.append(relationship)
        cls.tryinsert(relationships)

class FundRelation(BaseModel):
    business = ForeignKeyField(OtherBusiness)
    privatefund = ForeignKeyField(PrivateFund)

    class Meta:
        db_table = 'fund_relationships'
        indexes = (
            (('business', 'privatefund'), True),
                )

    @classmethod
    def create_relationships(cls, crd, funds):
        relationships = []
        for fund in funds:
            if 'businesses' not in fund:
                continue

            businesses = fund['businesses']
            for businessrow in businesses:
                dbfund = PrivateFund.addfund(crd, fund)
                relationship = OtherBusiness.create_relationship(
                    {'privatefund' : dbfund.id}, businessrow)
                relationships.append(relationship)

        if relationships:
            cls.tryinsert(relationships)

setup()
