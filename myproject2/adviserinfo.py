import re, zipfile
from xlrd import XLRDError
from functools import partial
import numpy as np
import pandas as pd
from stagelib import OSPath, Folder, to_single_space, from_json, to_json, mkpath, parsexml
from stagelib import mkdir as newfolder
from stagelib.stage import Stage

DATADIR = 'data'
to_datafolder = partial(newfolder, DATADIR)

formadvfolder = to_datafolder('formadv')
zipfolder = newfolder(formadvfolder, 'zipfiles')
xmlfolder = newfolder(formadvfolder, 'dailyxml')

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
    "251-500" : 500
		}

re_NUMBERSPECIFY = re.compile('^More than')

def get_filingdate(path):
    return pd.to_datetime(re.sub(r'^.*?ia(\d+)\.zip', r'\1', path))

def read_formadv(path):
    kwds = dict(true_values = 'Y', false_values = 'N')
    with zipfile.ZipFile(path) as zf:
        fobj = zf.open(zf.filelist[0].filename)
        try:
            return pd.read_excel(fobj, **kwds)
        except XLRDError:
            kwds.update({'delimiter' : '|', 'quoting' : 1, 'low_memory' : False})
            while True:
                try:
                    return pd.read_csv(fobj, **kwds)
                except pd.parser.CParserError:
                    kwds.pop('delimiter')
                    kwds['error_bad_lines'] = False
        finally:
            fobj.close()

def dailyxml2df():
    rows = parsexml(Folder.listdir(xmlfolder)[0], 'Firm', 'FormInfo')
    return pd.DataFrame(rows)

class FormADVStage(Stage):
    FIELDSPATH = mkpath('config', 'fieldsconfig.json')
    def __init__(self):
        super(FormADVStage, self).__init__('formadv')

    @staticmethod
    def get_numberofclients(df):
        data = df.numberofclients.copy()
        notnull = data.notnull()
        data.loc[data.contains(re_NUMBERSPECIFY)] = np.nan
        __ = df.loc[notnull, 'numberofclients_specify']
        return data.modify(notnull,
            data.fillna(__)).quickmap(numericrank)

    @staticmethod
    def get_description(description, key):
        if description.startswith(key):
            return ' '.join(i.capitalize() for i in
                description.replace("{}_".format(key), '').split('_'))
        return to_single_space(description)

    @staticmethod
    def get_categories(df):
        categories = ['client_types', 'compensation', 'pct_aum', 'disclosures']
        fields = ['adviser_id', 'description', 'specific', 'percentage']
        categorymap = {}
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
                description = data.level_1.modify(
                    mask_o, data.level_0.map(map_s)
                        ).quickmap(get_description, key), #
                specific = data.level_1.modify(mask_o, True, elsevalue = False), #
                adviser_id = data.level_0.map(df.crd.to_dict()), #
                percentage = data[0].quickmap(percentrank)#
                    ).ix[:, fields]

            dropmask = (descriptions.description != 'Other Specify') &\
                       (descriptions.percentage != 0)
            categorymap.update({
                key : descriptions.loc[dropmask].dropna()
                    })

        return categorymap

    #def _getfunc(self, name):
    #    pass

    def normdf(self, df, formadv_id = None, **kwds):
        df = super(FormADVStage, self).normdf(df, **kwds)
        return df.assign(formadv_id = formadv_id,
            numberofclients = self.get_numberofclients(df),)

self = FormADVStage()

