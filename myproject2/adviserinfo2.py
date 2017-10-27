import re
from functools import partial
import numpy as np
import pandas as pd
from stagelib import OSPath, Csv, Folder, to_single_space, mergedicts, from_json, to_json, mkpath, parsexml, readtable
from stagelib.web import HomeBrowser
import stagelib.record
from stagelib.stage import Stage

from db import newfolder
import db

formadv_folder = newfolder('data', 'formadv')
preprocessed_folder = newfolder(formadv_folder, 'preprocessed')
zipfolder = newfolder(formadv_folder, 'zipfiles')
xmlfolder = newfolder(formadv_folder, 'dailyxml')

companyfolder = partial(newfolder, 'data')

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
    "11-50" : 50,
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

def download_formadvs():
    br = HomeBrowser(starturl = r'https://www.sec.gov/help/foiadocsinvafoiahtm.html')
    for linktag in br.filterlinks(r'\d{6}\.zip'):
        url = linktag.url
        link = "https://www.sec.gov/%s" % url
        _ = OSPath.split(link)[-1]
        br.download(link, outfile = mkpath(zipfolder, _))

def get_dailyxml():
    pass #download gzip and extract as xmlfile

def get_filingdate(path):
    return pd.to_datetime(re.sub(r'^.*?ia(\d+)\.zip', r'\1', path))

def list_formadvs():
    return sorted([
        {'date' : get_filingdate(path), 'filename' : path}
        for path in Folder.listdir(zipfolder)
            ], key = lambda k: k['date'])

def read_formadv(path, **kwds):
    return readtable(path,
        true_values = 'Y',
        false_values = 'N',
        na_values = ['NONE'])

def read_dailyxml():
    return pd.DataFrame(parsexml(Folder.listdir(xmlfolder)[0], 'Firm', 'FormInfo'))

def get_outfile(formadv):
    return FormadvStage.get_outfile(formadv.date)

class FormadvStage(Stage):
    FIELDSPATH = mkpath('config', 'fieldsconfig.json')
    def __init__(self):
        super(FormadvStage, self).__init__('formadv')

    @staticmethod
    def get_outfile(date):
        return mkpath(preprocessed_folder, date.strftime("%m%d%y_output.csv"))
        
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

    def normdf(self, df, formadv_id = None, **kwds):
        df = super(FormadvStage, self).normdf(df, **kwds)
        return df.assign(
            formadv = formadv_id,
            adviser = df.crd,
            numberofclients = self.get_number(df),
            numberofemployees = self.get_number(df, field = 'numberofemployees')
                ).clean_addresses().addnames()

    def writefile(self, df, outfile, **kwds):
        while True:
            try:
                df.to_csv(outfile, index = False, **kwds); break
            except UnicodeEncodeError as e:
                self.error("Encoding troubles"); self.error(e)
                kwds['encoding'] = 'utf-8'

    def normfile(self, path = '', dailyxml = False, **kwds):
        if dailyxml:
            df = read_dailyxml()
            date = utcnow()
        elif path:
            df = read_formadv(path)
            date = get_filingdate(path)

        df = self.normdf(df, **kwds)
        self.writefile(df, self.get_outfile(date))
        return df

def processfiles(start = 1):
    advparser = FormadvStage()
    advparser.info("Starting at entry number {}".format(start))
    for formadv in db.FormADV.select():
        if formadv.id >= start:
            advparser.info("Currently processing '{}'".format(formadv.filename))
            advparser.normfile(path = formadv.filename, formadv_id = formadv.id)

def insertdata(start = 1):
    infotables = [db.SecFiler, db.Person, db.Phone, db.Fax, db.Address, db.Website, db.Numbers]
    desctables = {
        'client_types' : db.ClientType,
        'compensation' : db.Compensation,
        'pct_aum' : db.ClientTypeAUM,
        'disclosures' : db.Disclosure
            }

    for formadv in db.FormADV.select():
        if formadv.id >= start:
            df = FormadvStage.addnames(
                readtable(get_outfile(formadv))
                    ).assign(date = formadv.date)
    
            db.Adviser.insertdf(df)
            db.Filing.insertdf(df, extrafields = [])
            idmap = db.Filing.getdict(formadv)
            df['filing'] = df.crd.map(idmap)
    
            for table in infotables:
                inserted = table.insertdf(df, chunksize = 5000)
    
            typesmap = FormadvStage.get_types(df)
            if not typesmap:
                continue

            db.Description.tryinsert(typesmap['descriptions'])
            textdict = db.Description.textdict()

            for category, typestable in desctables.items():
                if category in typesmap:
                    typesdata = typesmap[category]
                    typesdata = typesdata.assign(
                        description = typesdata.description.map(textdict),
                        filing = typesdata.adviser.map(idmap))
                    typestable.insertdf(typesdata)

def setup():
    #download_formadvs()
    db.FormADV.tryinsert(list_formadvs())
    #processfiles()
    insertdata()

pd.DataFrame.addnames = FormadvStage.addnames

if __name__ == '__main__':
    setup()
