import os, re, gzip
import numpy as np
import pandas as pd
from stagelib import (ospath, File, Csv, Folder, joinpath,
                      to_single_space, mergedicts, chunker,
                      floating_point, readjson, writejson)
import stagelib.record
from stagelib.stage import Stage

re_PERCENTAGE = re.compile(r'(^\d+)%.*?$')
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

class FormadvStage(Stage):
    def __init__(self, formadv_id = None):
        super(FormadvStage, self).__init__('formadv', fieldspath = r'config/fieldsconfig.json')
        if formadv_id:
            if isinstance(formadv_id, int):
                self.formadv = FormADV.get(id = formadv_id)
            else:
                self.formadv = formadv_id

    @classmethod
    def processfiles(cls, start = 1, **kwds):
        advprsr = cls()
        advprsr.info("Starting at entry number {}".format(start))
        for formadv in FormADV.select():
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

    def parse(self, df, **kwds):
        df = super(FormadvStage, self).parse(df, **kwds)
        num = df[self.numeric_fields].copy()
        if num.any(axis = 1).any():  #these did not provide a value
            df[self.numeric_fields] = num.fillna(0)

        return df.assign(
            formadv = self.formadv.id,
            adviser = df.crd,
            numberofclients = self.get_number(df),
            numberofemployees = self.get_number(df, field = 'numberofemployees'),
            date = formadv.date,
                ).addnames()

pd.DataFrame.addnames = FormadvStage.addnames
