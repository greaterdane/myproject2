import re
import shutil

from adviserinfo import *

FIELDS_PATH = os.path.join(adv.CONFIGDIR, 'fields_map', 'advfiling_fields_map.json')

PERCENT_RANKINGS = {
	u"0 percent": 0,
	u"100 percent": 6,
	u"11-25 percent": 2,
	u"Up to 25 percent": 2,
	u"26-50 percent": 3,
	u"Up to 50 percent":3,
	u"51-75 percent": 4,
	u"Up to 75 percent":4,
	u"76-99 percent": 5,
	u"More than 75 percent": 5,
	u"Up to 10 percent": 1,
	u"1-10 percent": 1,
        }

NUMERIC_RANKINGS = {
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
    "More than 1000": 1001,
		}

re_TENDIGITS = re.compile(r'^\d{10}$')
phoneformat = "({}) {}-{}".format
usdformat = '${:,.2f}'.format

def get_aumdiff(df1, df2):
    aumdict = df2.get_mapper('crd', 'assetsundermgmt')
    return df1.assetsundermgmt - df1.crd.quickmap(aumdict)

def to_phone(series):
    series_copy = series.str.replace(r'\(|\)|-|\.| |\/', '')
    return series_copy.modify(
        series_copy.contains(re_TENDIGITS),
        series_copy.to_ascii().quickmap(lambda x: phoneformat(x[0:3], x[3:6], x[6:])),
        series)

def to_usd(series):
    return series.quickmap(usdformat)

class AdvFiling(StageTable):
    ADVDIR = mkdir(adv.DATADIR, 'formadv')
    ZIPDIR = mkdir(ADVDIR, 'zipfiles')
    EXTRACTDIR = mkdir(ADVDIR, 'unzipped')
    PROCESSED = mkdir(ADVDIR, 'processed')
    IMPORTED = mkdir(ADVDIR, 'imported')
    DESCDIR = mkdir(IMPORTED, 'descriptions')
    LISTDIR = mkdir(adv.BASEDIR, 'lists')
    COMPANYLIST_PATH = mkpath(LISTDIR, 'companylist.csv')
    PREVIOUSQTR_PATH = mkpath(LISTDIR, 'previousqtr.csv')

    def __init__(self, path, fields_path = FIELDS_PATH, table = 'advfiling', **kwds):
        super(AdvFiling, self).__init__(path, fields_path = fields_path, table = table, **kwds)
        self.outfile = mkpath(self.PROCESSED, ospath.basename(self.outfile))

    @classmethod
    def get_formadvs(cls):
        br = HomeBrowser(starturl = r'https://www.sec.gov/help/foiadocsinvafoiahtm.html')
        for link_tag in br.filter_links(r'\d{6}\.zip'):
            url = link_tag.url
            link = "https://www.sec.gov/%s" % url
            br.download(link, output_file = mkpath(cls.ZIPDIR, ospath.split(url)[-1]))

    @classmethod
    def unzip_formadvs(cls):
        Folder.unzipfiles(cls.ZIPDIR, extractdir = cls.EXTRACTDIR)

    @classmethod
    @dbfunc(IapdDB)
    def enter_formadvs(cls, db):
        fa = db.formadv()
        cls.unzip_formadvs()
        unzipped = cls.conform(Folder.table(cls.EXTRACTDIR),
            fa.table, #<-- table is accessed or created here
            learn = True,
            login = db._login)\
                .sort_values(by = 'filingdate')
        db.insert(unzipped.drop_blank_fields()\
            .assign(output = unzipped.filename.map(
                lambda x: "%s_output.csv" % re.sub(r'\.\w+$', '', x)
                    )), fa.table)
    @classmethod
    @dbfunc(IapdDB)
    def get_companylist(cls, db):
        return db.select_grouped_aggregate('advfiling',
            'crd','formadv_id')\
                .fillna('')\
                .apply(lambda x: x.to_ascii()\
                .astype(str)
                    if 'company' in x.name else x)

    @classmethod
    @dbfunc(IapdDB)
    def store_most_recent_filings(cls, db):
        data = cls.get_companylist()
        data = data.assign(fax = data.fax.to_phone(), phone = data.phone.to_phone())
        max_ids = map(str, data.id)
        other_ids = db.select('advfiling',
            fields = ['id', 'crd', 'formadv_id'],
            subquery = "where id not in (%s)" % ', '.join(max_ids)
                )

        tings = other_ids\
            .groupby(['crd'], as_index = False)\
            .agg({'formadv_id' : 'max'})

        dflist = []
        tg = tings.groupby('formadv_id')
        fg = other_ids.groupby('formadv_id')
        for f_id, group in tg:
            df = fg.get_group(f_id)
            dflist.append(df.loc[df.crd.isin(group.crd)])
    
        ids = pd.concat(dflist).id
        dataprev = db.select('advfiling',
            subquery = "where id in (%s)" % ', '.join(map(str, ids))
                )
        dataprev = dataprev.assign(fax = dataprev.fax.to_phone(), phone = dataprev.phone.to_phone())
        
        results_to_csv(cls.COMPANYLIST_PATH,
            data.assign(aumdiff = get_aumdiff(data, dataprev),
                company = data.company.str.strip('"'))\
                    .drop_duplicates(subset = ['crd', 'company']))

        results_to_csv(cls.PREVIOUSQTR_PATH, dataprev)

    @classmethod
    @dbfunc(IapdDB)
    def store_descdata(cls, db):
        for table in db.desctables:
            outfile = ospath.join(cls.LISTDIR, table + ".csv")
            if table != 'disclosures':
                results_to_csv(outfile,
                    db.select_grouped_aggregate(table, 'crd','formadv_id'))
            else:
                results_to_csv(outfile, db.select(table))

    @classmethod
    @dbfunc(IapdDB)
    def process_formadvs(cls, db):
        entries = db.select('formadv',
            subquery = 'where rows_original is NULL')

        for row in entries.itertuples():
            self = cls(mkpath(cls.EXTRACTDIR, row.filename))
            for df in self:
                df['formadv_id'] = row.id
                self.write(df)

            descmap = db.description_map
            for k, v in self.descriptions.items():
                v['formadv_id'] = row.id
                v['desc'] = v.desc\
                    .str.replace('"', '')\
                    .to_ascii()\
                    .textclean()\
                    .clean()

                newdesc = v.loc[(v.desc != '') & ~(v.desc.isin(descmap.keys()))].dropna()
                newdesc['type'] = newdesc.desc.modify(
                    newdesc.desc.contains(r'[a-z]'), 1, 2)

                if not newdesc.empty:
                    db.insert(newdesc.assign(id = np.nan)\
                        .ix[:, ['id', 'desc', 'type']], 'descriptions')

                descfile = ospath.join(cls.DESCDIR, "%s_%s.csv" % (row.id, k))
                results_to_csv(descfile, v.loc[v.value > 0]\
                    .assign(desc = v.desc.quickmap(db.description_map)))

                if k != 'advisory_activities':
                    db.load_csv(descfile, k)

                if not ospath.exists(descfile):
                    shutil.move(descfile, descdir)

            db.affectrows("update formadv set rows_original =  %s where id = %s"\
                % (self.file.nrows, row.id))
            cls.load_formadv(row)

        cls.store_most_recent_filings()
        cls.store_descdata()

    @classmethod
    @dbfunc(IapdDB)
    def load_formadv(cls, row, db):
        outfile = mkpath(cls.PROCESSED, row.output)
        rows_imported = db.load_csv(outfile, 'advfiling')
        q = "update formadv set rows_imported =  %s where id = %s"\
            % (rows_imported, row.id)

        db.affectrows(q)
        shutil.move(outfile, cls.IMPORTED)
    
    @staticmethod
    def normalize_columndesc(columndesc, key):
        return ' '.join(i.capitalize() if '_'
            in columndesc else i for i in
            re.sub(r'%s_' % key, r'', columndesc).split('_'))

    @staticmethod
    def get_numclients(df):
        if not df.filter(regex = 'numberofclients').empty:
            return df.numberofclients\
                .modify(df.numberofclients.contains(re.compile(
                    'more.*?(?:100|500)', re.I)), np.nan)\
                .fillna(df.numberofclients_specify)\
                .quickmap(PERCENT_RANKINGS)\
                .to_numeric()

    @staticmethod
    def get_checkbox_answers(df):
        filters = ['client_types', 'compensation',
                   'advisory_activities','pct_aum',
                   'disclosures'] #'number_of_clients.*?year
        typedict = {}
        for key in filters:
            data = df\
                .filter(regex = key)\
                .stack()\
                .reset_index()
            if data.empty:
                continue

            _dict = data.loc[
                data.level_1.contains('_specify')
                    ].get_mapper('level_0', 0)

            _dict2 = data.loc[
                data.level_1.contains('_other$')
                    ].get_mapper('level_0', 0)
            
            data = data.assign(
                desc = data.level_1.modify( #
                    data.level_1.contains('_specify'),
                    data.level_0.map(_dict))\
                        .quickmap(AdvFiling\
                        .normalize_columndesc, key)\
                        .fillna('OTHER'),
                crd = data.level_0\
                    .map(df.crd.to_dict()),
                value = data.level_1.modify( #
                    data.level_1.contains('_specify$'),
                    data.level_0.map(_dict2),
                    elsevalue = data[0])\
                        .astype(str)\
                        .replace({'^Y$' : True,'^N$' : False},
                            regex = True)\
                        .quickmap(PERCENT_RANKINGS)\
                        .to_numeric()
                            ).ix[:,['crd', 'desc', 'value']].dropna()

            typedict.update({
                key : data.loc[~(data.desc == 'Other')]
                    })

        return typedict

    @property
    def companylist(self):
        if not ospath.exists(self.COMPANYLIST_PATH):
            self.store_companylist()
        return pd.read_csv(self.COMPANYLIST_PATH)

    def normalize(self, df, *args, **kwds):
        df = df.rename(columns = self.fields_map)\
            .mangle_cols()\
            .assign(**{
                k : df.combine_dup_cols(k) for k in df.dup_cols()
                    })

        self.descriptions = self.get_checkbox_answers(df)
        df = super(AdvFiling, self).normalize(
            df.assign(numberofclients = self.get_numclients(df),
                phone = df.phone.to_phone()),
                    *args, **kwds)

        if hasattr(df, 'fax'):
            df['fax'] = df['fax'].to_phone()

        try:
            df['numberofemployees'] = df.numberofemployees.quickmap(NUMERIC_RANKINGS)
        except KeyError:
            pass
        return df

pd.Series.to_usd = to_usd
pd.Series.to_phone = to_phone

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument('--update', action = 'store_true', default = False)
    parser.add_argument('--load', action = 'store_true', default = False)
    
    args = parser.parse_args()
    if args.update:
        AdvFiling.store_most_recent_filings()
        AdvFiling.store_descdata()
    elif args.load:
        AdvFiling.process_formadvs()
        
    