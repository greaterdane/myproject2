from collections import OrderedDict
import os
from formadv import *

TABLEFIELDS = ['crd', 'company', 'numberofclients',
               'numberofaccts', 'numberofemployees',
               'assetsundermgmt', 'aumdiff']

DESCFIELDS = ['desc', 'value',  'formadv_id']
STREETFIELDS = ['address1', 'address2']
REGIONFIELDS = ['city', 'state', 'zip']
ADDRFIELDS = {'staddr' : STREETFIELDS, 'region' : REGIONFIELDS}
CONTACTFIELDS = ['contactperson', 'phone', 'fax', 'url']
INFOFIELDS = ['legalname', 'latestfilingdate']
NUMERICFIELDS = ['assetsundermgmt', 'numberofaccts', 'numberofclients', 'numberofemployees']
NUMERICFIELDS_WITH_ID = NUMERICFIELDS + ['formadv_id']
PERCENTMAP = {v : k.replace('percent', '%').replace("More", ">") for k, v in PERCENT_RANKINGS.items() if k.startswith(('Up', 'More', '100'))}
REVPERCENTMAP = {v : k for k, v in PERCENTMAP.items()}
CATEGORIESMAP = {'pct_aum' : 'AUM% / Client Type', 'client_types' : 'Types of Clients', 'compensation': 'Compensation', 'disclosures' : 'Reported Disclosures'}

def getdescdata():
    desctables = {}
    for table in db.desctables:
        desctables.update({
            table : pd.read_csv(os.path.join(AdvFiling.LISTDIR, table + ".csv"))
                })

    return desctables

def get_percentage_filters():
    """Get all descriptions that reflect a percentage , e.g. client_types, pct_aum, etc. for filter menu."""
    cdict = [
        ['pct_aum',(1, 13,)],
        ['client_types',(1, 13,)],
        ['compensation',(15, 19,)],
        #'disclosures' : (2782, 2797,),
            ]

    percentages = OrderedDict()
    for i, k in enumerate(sorted(PERCENTMAP)):
        percentages.update({i :PERCENTMAP[k]})

    filtermap = OrderedDict()
    for (category, idxrng) in cdict:
        keys = db.select('descriptions',
            fields = ['id'],
            subquery = "where id >= {} and id <= {}".format(*idxrng)
                    ).iloc[:, 0].values

        filtermap.update({
            category : {
                'name' : CATEGORIESMAP[category],
                'data' : {k : {
                    'percentages' : percentages,
                    'name' : REVDESCMAP[k]
                        } for k in keys
                            }}})
    return filtermap

def color_negative_red(val):
    color = 'red' if val < 0 else 'black'
    return 'color: %s' % color

def formatlink(rooturl, x):
    return '<a href="{0}{1}">{2}</a>'.format(rooturl, x, CRDMAP[x])

def formatnumeric(numeric):
    if not numeric.empty:
        gt0mask = numeric[NUMERICFIELDS]\
            .apply(lambda x: x > 0)\
            .all(axis = 1)
        return numeric.loc[gt0mask]
    return numeric

def formatdesc(desc):
    return desc.assign(
        desc = desc.desc.map(REVDESCMAP), #
        percentage = desc.value.modify( #
                desc.category != 'disclosures',
                desc.value.quickmap(PERCENTMAP)),
        category = desc['category'].quickmap(CATEGORIESMAP))

def formataddress(listing):
    for k, fields in ADDRFIELDS.items():
        listing[k] = ' '.join((
            listing[i] + "," if i == 'city' else listing[i])
                for i in fields if listing[i])
    return listing

def to_styled_html(df):

    styles = [ 
        dict(selector = ".col0", props = [('display', 'none')]), 
        dict(selector = "th:first-child", props = [('display', 'none')])
            ]

    return df.style\
        .set_table_attributes('class= "niceTable"')\
        .set_table_styles(styles)\
        .applymap(color_negative_red, subset = ["aumdiff"])\
        .render()

def mostrecent(df):
    return df.loc[df.date == df.date.max()]

def getlisting(crd):
    return formataddress(
        data.loc[data.crd == crd]\
            .T.iloc[:, 0]\
            .fillna('')\
            .to_dict())

def is_disclosure(desc):
    return desc.category.contains('disclosures', case = False)

def getdisclosures(desc):
    return desc.loc[is_disclosure]

def getdata(crd):
    desc = pd.DataFrame()
    filings.id = crd

    for k, v in desctables.items():
        desc = desc.append(v.loc[v.crd == crd].assign(category = k))

    numeric = formatnumeric(filings.select(fields = NUMERICFIELDS_WITH_ID))
    desc = formatdesc(desc)

    __ = {}
    datalist = [
        (desc, 'desc', DESCFIELDS + ['category']),
        (numeric, 'numeric', NUMERICFIELDS_WITH_ID),
            ]

    for df, name, fields in datalist:
        if df.empty:
            df = pd.DataFrame({
                'formadv_id' : formadvs.id
                    }).ix[:, fields]
        __.update({
            name : df.assign(
                date = df.formadv_id.map(DATEMAP)
                    ).sort_values(by = 'date').fillna('n/a')
                        })
    __.update({
        'listing' : getlisting(crd),
        'disclosures' : getdisclosures(__['desc']),
        'desc' : mostrecent(__['desc'].loc[~is_disclosure(__['desc'])])
            })

    for k in __:
        try:
            del __['formadv_id']
        except KeyError:
            pass
    return __

def filter_descriptions(table = 'client_types', desc2rank = {}, resetcache = False):
    for description, ranking in desc2rank.items():
        #sfunc = isearch(REVDESCMAP[int(description)])
        #rdesc = {k : v for k,v in REVDESCMAP.items() if sfunc(str(v))}
        descdata = desctables[table]
        mask = (descdata['desc'] == int(description)) & (descdata['value'] == int(ranking))
        result = descdata.loc[mask]
        yield result

db = IapdDB()
DESCMAP = db.description_map
REVDESCMAP = {v : k for k, v in DESCMAP.items()}

#REVDESCMAP_CLIENT_TYPES = {k : v for k, v in REVDESCMAP.items() if k in in_client_types}

formadvs = db.formadv().records
DATEMAP = formadvs.get_mapper('id', 'filingdate')
desctables = getdescdata()
filings = db.advfiling(id_field = 'crd')

data = pd.read_csv(AdvFiling.COMPANYLIST_PATH)
data.sort_values(['aumdiff', 'assetsundermgmt'],
    ascending = [1, 0],
    inplace = True)

dataprev = pd.read_csv(AdvFiling.PREVIOUSQTR_PATH)

CRDMAP = data.get_mapper('crd', 'company')

indextable = data.ix[:,TABLEFIELDS].assign(
    assetsundermgmt = data['assetsundermgmt'].to_usd(),
    aumdiff = data.aumdiff.to_usd().str.strip('$'))

