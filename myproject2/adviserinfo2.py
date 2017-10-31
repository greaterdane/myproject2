import os, re, gzip
from functools import partial
import numpy as np
import pandas as pd
from selenium import webdriver
from stagelib import (OSPath, Csv, Folder,
    utcnow, to_single_space, mergedicts,
    chunker, floating_point, appendData,
    from_json, to_json, mkpath,
    parsexml, readtable)

from stagelib.web import HomeBrowser, pause, USER_AGENT
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

def download_formadvs():
    br = HomeBrowser(starturl = r'https://www.sec.gov/help/foiadocsinvafoiahtm.html')
    for linktag in br.filterlinks(r'\d{6}\.zip'):
        url = linktag.url
        link = "https://www.sec.gov/%s" % url
        _ = OSPath.split(link)[-1]
        br.download(link, outfile = mkpath(zipfolder, _))

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

def get_dailyxml_path():
    return OSPath.abspath(
        mkpath(xmlfolder,
            utcnow().strftime(r'IA_FIRM_SEC_Feed_%m_%d_%Y.xml.gz')
                ))

def get_dailyxml():
    xmlpath = get_dailyxml_path()
    if OSPath.exists(xmlpath):
        os.remove(xmlpath)

    link = HomeBrowser(
        r'https://www.adviserinfo.sec.gov/IAPD/InvestmentAdviserData.aspx'
            ).find_link(text = r'SEC Investment Adviser Report')

    profile = webdriver.FirefoxProfile()
    profile.set_preference("general.useragent.override", USER_AGENT)
    profile.set_preference("browser.download.folderList", 2)
    profile.set_preference("browser.download.manager.showWhenStarting", False)
    profile.set_preference("browser.download.dir", OSPath.abspath(xmlfolder))
    profile.set_preference("browser.helperApps.neverAsk.saveToDisk", r'application/x-gzip')
    driver = webdriver.Firefox(profile)
    driver.get(r'https://www.adviserinfo.sec.gov/IAPD/InvestmentAdviserData.aspx')
    driver.execute_script(link.attrs[3][1])
    pause(100, 500)
    driver.close()

def read_dailyxml():
    
    f = gzip.open(get_dailyxml_path(), 'rb')
    for chunk in chunker(f, chunksize = 100):
        appendData(mkpath(xmlfolder, 'daily.xml'), ''.join(chunk))
    f.close() #catch IOerror
            
    return pd.DataFrame(parsexml('dailyxml.xml', 'Firm', 'FormInfo'))

def get_outfile(formadv):
    return FormadvStage.get_outfile(formadv.date)

def processfiles(start = 1):
    advparser = FormadvStage()
    advparser.info("Starting at entry number {}".format(start))
    for formadv in db.FormADV.select():
        if formadv.id >= start:
            advparser.info("Currently processing '{}'".format(formadv.filename))
            advparser.normfile(path = formadv.filename, formadv_id = formadv.id)

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

def insertdata(start = 1):
    parser = FormadvStage()
    infotables = [db.Person, db.Phone, db.Fax, db.Address, db.Website, db.Numbers]
    desctables = {
        'client_types' : db.ClientType,
        'compensation' : db.Compensation,
        'pct_aum' : db.ClientTypeAUM,
        'disclosures' : db.Disclosure
            }

    for formadv in db.FormADV.select():
        if formadv.id >= start:
            df = FormadvStage.addnames(
                readtable(get_outfile(formadv), encoding = 'latin')
                    ).assign(date = formadv.date)
            
            df['numberofemployees'] = FormadvStage.get_number(df, field = 'numberofemployees')
            df['numberofclients'] = FormadvStage.get_number(df, field = 'numberofclients')
            numericdata = df[parser.numeric_fields].copy()
            if any(numericdata.any(axis = 1)):
                df[parser.numeric_fields] = numericdata.fillna(0)

            db.Adviser.insertdf(df)
            db.Filing.insertdf(df, extrafields = [])
            idmap = db.Filing.getdict(formadv)
            df['filing'] = df.crd.map(idmap)
            db.SecFiler.insertdf(df, extrafields = ['adviser'])

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
            
                    typesdata = typesdata.loc[
                        ~typesdata.description.contains(r'^$|^ +$')
                            ]
            
                    typestable.insertdf(typesdata, extrafields = ['filing'])

def setup():
    #download_formadvs()
    db.FormADV.tryinsert(list_formadvs())
    #processfiles()
    insertdata()

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

pd.DataFrame.addnames = FormadvStage.addnames

if __name__ == '__main__':
    setup()
