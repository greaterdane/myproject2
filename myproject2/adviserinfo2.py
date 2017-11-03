import os, re, gzip
import numpy as np
import pandas as pd
from stagelib import (OSPath, Csv, Folder,
    to_single_space, mergedicts,
    chunker, floating_point, appendData,
    from_json, to_json, mkpath,
    parsexml, readtable)

import stagelib.record
from scraper import download_formadvs, get_dailyxml_path, download_dailyxml
from stagelib.stage import Stage

from db import newfolder
import db

re_PERCENTAGE = re.compile(r'(^\d+)%.*?$')

INFOMODELS = [db.Person, db.Phone, db.Fax, db.Address, db.Website, db.Numbers]
DESCRIPTION_MODELS = {
    'client_types' : db.ClientType,
    'compensation' : db.Compensation,
    'pct_aum' : db.ClientTypeAUM,
    'disclosures' : db.Disclosure
        }

SCHEDULE_D_MODELS = {
    'people' : db.Person,
    'funds' : db.PrivateFund,
    'regulatory_drps' : db.Courtcase,
        }

formadv_folder = newfolder('data', 'formadv')
zipfolder = newfolder(formadv_folder, 'zipfiles')
xmlfolder = newfolder(formadv_folder, 'dailyxml')
preprocessed = newfolder(formadv_folder, 'preprocessed')

percentrank = {
	u"0 percent": 0,
	u"100 percent": 1,
	u"11-25 percent": .25,
	u"Up to 25 percent": .25,
	u"26-50 percent": .5,
	u"Up to 50 percent": .5,
	u"51-75 percent": .75,
	u"Up to 75 percent": .75,
	u"76-99 percent": .75,
	u"More than 75 percent": .80,
	u"Up to 10 percent": .1,
	u"1-10 percent": .1,
        }

numericrank = {
    "1-10" : 10,
    "6-10" : 10,
    "1-5" : 5,
    "11-25" : 25,
    "1-25" : 25,
    "11-50" : 50,
    "Nov-50" : 50,
    "50-250" : 250,
    "251-500" : 500,
    "500-1000" : 1000,
    "26-100" : 100,
    "101-250" : 250,
    "251-500" : 500,
    "501-1000" : 1000,
    "51-250" : 250
		}

re_NUMBERSPECIFY = re.compile('^More than')

def get_filingdate(path):
    return pd.to_datetime(re.sub(r'^.*?ia(\d+)\.zip', r'\1', path))

def list_formadvs():
    return sorted([
        {'date' : get_filingdate(path), 'filename' : path}
        for path in Folder.listdir(zipfolder)
            ], key = lambda k: k['date'])

def read_formadv(formadv, **kwds):
    return readtable(formadv.filename,
        true_values = 'Y',
        false_values = 'N',
        na_values = ['NONE'])

def read_dailyxml():
    f = gzip.open(get_dailyxml_path(), 'rb')
    for chunk in chunker(f, chunksize = 100):
        appendData(mkpath(xmlfolder, 'daily.xml'), ''.join(chunk))
    f.close() #catch IOerror
    return pd.DataFrame(parsexml('dailyxml.xml', 'Firm', 'FormInfo'))

##this needs to be rewritten to extract and parse schedule D's from the source.
def parse_scheduleDjson(crd):
    data = []
    try:
        adviser = db.Adviser.get(crd = crd)
        funds = adviser.privatefunds
    except (IOError, db.Adviser.DoesNotExist, KeyError):
        return data

    for fund in funds:
        row = {
            'crd' : crd,
            'adviser' : adviser.name,
            'fund_id' : fund['fund_id'],
            'fundname' : fund['name'],
            'assetsundermgmt' : floating_point(fund['fundinfo'].get('assetsundermgmt', 'n/a')),
            'fundtype' : fund['fundinfo'].get('fundtype', 'n/a'),
            'owners' : fund['fundinfo'].get('numberofowners', 'n/a'),
                }

        if 'businesses' in fund:
            for business in fund['businesses']:
                data.append(mergedicts(business, row))
    return data

def load_formadv(formadv): #formadv db entry
    df = readtable(formadv.outfile, encoding = 'latin')
    db.Adviser.insertdf(df)
    db.Filing.insertdf(df, extrafields = [])
    idmap = db.Filing.getdict(formadv)
    df['filing'] = df.crd.map(idmap)
    db.SecFiler.insertdf(df, extrafields = ['adviser'])

    for table in INFOMODELS:
        table.insertdf(df, chunksize = 5000)
    
    typesmap = FormadvStage.get_types(df)
    if typesmap:
        db.Description.tryinsert(typesmap['descriptions'])
        textdict = db.Description.textdict()
        
        for category, typestable in DESCRIPTION_MODELS.items():
            if category in typesmap:
                typesdata = typesmap[category]
                typesdata = typesdata.assign(
                    description = typesdata.description.map(textdict),
                    filing = typesdata.adviser.map(idmap))
        
                typesdata = typesdata.loc[
                    ~typesdata.description.contains(r'^$|^ +$')
                        ]

                typestable.insertdf(typesdata, extrafields = ['filing'])    

def load_formadvs(start = 1):
    for formadv in db.FormADV.select():
        if formadv.id >= start:
            load_formadv(formadv)

def load_scheduleD(data):
    crd = int(data['crd'])
    innerdata = data['data']

    if 'businesses' in innerdata:
        db.AdviserRelation.create_relationships(crd, innerdata['businesses'])

    if 'funds' in data:
        db.FundRelation.create_relationships(crd, data['funds'])

    if 'people' in data:
        db.Person.addpeople(crd, data['people'])

    if 'regulatory_drps' in innerdata:
        db.Courtcase.addcases(crd, innerdata['regulatory_drps'])

def load_scheduleDs():
    folders = [
        folder for folder in Folder.listdir('data', pattern = r'\d+$')
            ]

    for folder in folders:
        try:
            data = from_json(mkpath(folder, 'predictiveops.json'))
            load_scheduleD(data)
        except IOError:
            continue

def setup():
    #download_formadvs()
    db.FormADV.tryinsert(list_formadvs())
    #FormadvStage.processfiles()
    load_formadvs()
    #re_CRD = re.compile(r'data[^\d+]+(\d+$)')

class FormadvStage(Stage):
    FIELDSPATH = mkpath('config', 'fieldsconfig.json')
    def __init__(self):
        super(FormadvStage, self).__init__('formadv')

    @classmethod
    def processfiles(cls, start = 1, **kwds):
        advprsr = cls()
        advprsr.info("Starting at entry number {}".format(start))
        for formadv in db.FormADV.select():
            if formadv.id >= start:
                advprsr.info("Currently processing '{}'".format(formadv.filename))
                advprsr.normfile(formadv)

    @staticmethod
    def get_number(df, field = 'numberofclients'):
        data = df[field].copy()
        mask = data.notnull()
        data.loc[data.contains(re_NUMBERSPECIFY)] = np.nan
        __ = df.loc[mask, '{}_specify'.format(field)]
        return data.modify(mask,
            data.fillna(__)).quickmap(numericrank)

    @staticmethod
    def cleantext(text, key):
        if text.startswith(key):
            return ' '.join(i.capitalize() for i in
                text.replace("{}_".format(key), '').split('_'))
        return to_single_space(text)

    @staticmethod
    def get_types(df):
        categories = ['client_types', 'compensation', 'pct_aum', 'disclosures']
        fields = ['adviser', 'text', 'specific', 'percentage']
        typesmap = {'descriptions' : []}
        for key in categories:
            data = df.filter(regex = key).stack().reset_index()
            if data.empty:
                continue

            __maps = {}
            for _key in ('specify', 'other',):
                __ = data.level_1.contains("{}_(?:other_)?{}$".format(key, _key))
                __maps.update({
                    _key : {'map' : data.loc[__].get_mapper('level_0', 0), 'mask' : __}
                        })

            mask_o = __maps['other']['mask']
            map_s = __maps['specify']['map']
            map_o = __maps['other']['map']

            descriptions = data.assign(
                text = data.level_1.modify(
                    mask_o, data.level_0.map(map_s)
                        ).quickmap(FormadvStage.cleantext, key), #
                specific = data.level_1.modify(mask_o, True, elsevalue = False), #
                adviser = data.level_0.map(df.crd.to_dict()), #
                percentage = data[0].quickmap(percentrank)#
                    ).ix[:, fields]

            qty = descriptions.percentage.to_numeric(force = True)
            if key == 'disclosures':
                descriptions['number'] = qty
            else:
                 descriptions['percentage'] = qty

            dropmask = (descriptions.text != 'Other Specify') & (qty != 0) & (qty.notnull())
            descriptions = descriptions.loc[dropmask].dropna()
            typesmap['descriptions']\
                .extend(descriptions\
                    .ix[:, ['text', 'specific']]\
                    .drop_duplicates(subset = ['text'])\
                    .to_dict(orient = 'records'))

            typesmap.update({
                key : descriptions.loc[dropmask]\
                    .dropna().rename(columns = {'text' : 'description'})
                    })

        typesmap['descriptions'] = [dict(t) for t in {
            tuple(d.items()) for d in typesmap['descriptions']
                }]

        return typesmap

    @staticmethod
    def addnames(df):
        if hasattr(df, 'contactperson'):
            df = pd.concat([df, df.contactperson.to_name()], axis = 1)
        return df

    def normdf(self, df, formadv, **kwds):
        df = super(FormadvStage, self).normdf(df, **kwds)
        nflds = self.numeric_fields
        num = df[nflds].copy()
        if _num.any(axis = 1).any():  #these did not provide a value
            df[nflds] = num.fillna(0)

        return df.assign(
            formadv = formadv.id,
            adviser = df.crd,
            numberofclients = self.get_number(df),
            numberofemployees = self.get_number(df, field = 'numberofemployees'),
            date = formadv.date,
                ).clean_addresses().addnames()

    def writefile(self, df, outfile, **kwds):
        while True:
            try:
                df.to_csv(outfile, index = False, **kwds); break
            except UnicodeEncodeError as e:
                self.error("Encoding troubles"); self.error(e)
                kwds['encoding'] = 'utf-8'

    def normfile(self, formadv, dailyxml = False, **kwds):
        df = read_formadv(formadv.filename)
        df = self.normdf(df, formadv, **kwds)
        self.writefile(df, mkpath(preprocessed, formadv.outfile))
        return df

pd.DataFrame.addnames = FormadvStage.addnames

if __name__ == '__main__':
    setup()
