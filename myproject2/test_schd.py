import re
from functools import partial
from stagelib.generic import merge_dicts, chunker

from scraper import *
irgx = partial(re.compile, flags = re.I)

matcher = {
    'name' : irgx('(?:primary business\s+)?name of'),
    'legalname' : irgx('^legal name of '),
    'city' : irgx(r'^city'),
    'state' : irgx('^state'),
    'country' : irgx('^country'),
    'crd' : irgx('crd'),
    'sec_number' : irgx('^8.*?-')
        }

def createrow(item, category, crd):
    row = {}
    for field, rgx in matcher.items():
        for d in item:
            searched = rgx.search(d['question'])
            if searched:
                value = d['answer']
                if field == 'sec_number':
                    value = d['question'].replace(' ', '')
                row.update({field : value})

    results = {}
    for k, v in matcher.items():
        results.update({k : (row[k] if k in row else None)})
        if k =='name' and (not results[k] or results[k] == 0):
            results[k] = item[0]['answer']
    identity = {
        'type' : category.rstrip('s').replace(' ', '').lower(),
        'advisercrd' : crd
            }

    return merge_dicts(results, identity)

def parse_schd_json(crd = None):
    rows = []
    dirname = 'data'
    if crd:
        dirname = ospath.join(dirname, str(crd))
    jsonfiles = Folder.listdir(dirname,
        recursive = True,
        pattern = '.json')

    jsonfiles = Folder.listdir('data/{}'.format(crd), )
    
    for j in jsonfiles: #jsonlister #
        data = from_json(j)
        if data:
            for category, datalist in data.items():
                split_idx = []
                split_data = []
                for i, item in enumerate(datalist):
                    if matcher['country'].search(item['question']):
                        split_idx.append(i + 1)
            
                for i2, idx in enumerate(split_idx):
                    first = i2
                    last = split_idx[i2]
                    if i2 > 0:
                        prev = split_idx[i2 - 1]
                        first = prev
                    sl = slice(first, split_idx[i2])
                    split_data.append(datalist[sl])
            
                for item in split_data:
                    rows.append(createrow(item, category, crd))
    if rows:
        return rows
    return

self = IapdBrowser()
self.logger = logging.getLogger()
self.logger.setLevel(logging.DEBUG)

if not self.logger.handlers:
    formatter = logging.Formatter("%(crd)s|%(levelname)s|%(asctime)s|%(message)s")

    fh = logging.handlers.RotatingFileHandler('adviserinfo_schD.log', encoding = 'utf-8')
    fh.setLevel(logging.DEBUG)
    fh.setFormatter(formatter)
    
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)

    self.logger.addHandler(fh)
    self.logger.addHandler(ch)

crd_iter = [(i.strip('"\n') for i in cg) for cg in File(r'lists\crd_list.csv', chunksize = 120)]
for i, crd_group in enumerate(crd_iter):
    for crd in crd_group:
        sl = self.get_summary_link(crd)
        self.open(sl)
        try:
            self.follow_link(text = "View Form ADV By Section")
            self.follow_link(text = "Schedule D")
            data = self.scheduleD()
            dirname = mkdir(adv.DATADIR, str(crd))
            to_json(mkpath(dirname, 'scheduleD.json'), data)
            self.back()
            self.logger.info("Sucessfully Browsed.", extra = {'crd' : crd})
        except (mechanize._mechanize.LinkNotFoundError) as e:
            self.logger.error("LinkNotFoundError", extra = {'crd' : crd})
        except URLError as e:
            self.logger.error(("URLError", e), extra = {'crd' : crd})
        except Exception as e:
            if self.status_code in [503, 403, 401]:
                self.logger.error((e, "Bad Status code: %s" % self.status_code), extra = {'crd' : crd})
                pause(4007, 5696)
            self.logger.error(e, extra = {'crd' : crd})
        pause(236, 396)

    rows = []
    for crd in crd_group:
        results = parse_schd_json(crd)
        if results:
            rows.extend(results)

    df = pd.DataFrame(rows)
    df = df.assign(
        crd = df['name'].map(df.get_mapper('name', 'crd'))
            ).drop_duplicates(['name', 'city', 'state', 'type'])
    df = df.loc[df.name != '0']
    df.to_csv('schd_{}.csv'.format(i),
        index = False,
        encoding = 'utf-8')
    pause(4007, 5696)
