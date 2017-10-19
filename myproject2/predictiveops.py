import re
from collections import defaultdict
from stagelib import dictupgrade, mergedicts
from stagelib import isearch
from stagelib.web import *

from adviserinfo import Company

directowner_search = isearch(r'^\n(?P<title>[A-Z]+.*?) +\(since (?P<since>\d+\/\d+)\)\n+Ownership Percentage: +(?P<ownership>.*?)\n')
plaintiff_search = isearch(r'^Plaintiff: +(?P<plaintiff>.*?$)')
address_search = isearch(r'^(?P<address>.*?)(?:\s+Phone:\s+|$)(?P<phone>[^\s]+)?(?:\s+Fax:\s+)?(?P<fax>[^\s]+)?')
location_topic = isearch(r'books|other office')
id_search = isearch(r'/(?P<id>\d+$)')
re_DESCRIPTION = re.compile(r'(^.*?)\. +The firm is based in ([A-Z].*?[A-Z]\.) As of.*?$')

re_FUNDINFODICT = dictupgrade({
    'fundtype' : '^(.*?)\n\n',
    'region' : '\n\n(.*?)\n\nGAV:',
    'gav' : 'GAV: (\$(?:\s+)?\d+.*?)\s+',
    'reportdate' : '\(reported: (\d{4}-\d{2}-\d{2})\)',
    'numberofowners' : '\n\n(\d+) Beneficial Owners',
    'pctclients_invested' : '(\d+)% of clients invested',
    'manager' : 'Managers: (.*?)\n\n',
    'feederfund' : 'Feeder fund name: (.*?)\n\n',
    'otheradvisers' : 'Other advisers: (.*?)$',
    }, isearch)

re_DRPDICT = dictupgrade({
    'filed' : 'Filed: (\d+\/\d+\/\d+)\s+',
    'docketnumber' : '(?:DOCKET (?:NO.|NUMBER)|Docket\/Case No.:)\s+([^\s]+)\s+',
    'court' : 'Court\/Case No\.:(?:\s+)?([A-Z]+.*?)(?:;)?\s+(?:DOCKET N)',
    'amendedfine' : 'Amended Fine: +(\$(?:\s+)?\d+.*?)\s+',
    'allegations' : 'Allegations:\s+([A-Z]+.*?)(?:$|\s+Judgement Rendered Fine.*?$)',
    'resolution' : 'Resolution Details:\s+(.*?)\s+Allegations:',
    'renderedfine' : 'Judgement Rendered Fine: +(\$(?:\s+)?\d+.*?)\s+',
    'sanctions' : 'Sanctions: +([A-Z]+.*?$)',
        }, isearch)

TOPICS = [
    'Books and Records Locations',
    'Administrators',
    'Auditors',
    'Custodians',
    'Prime Brokers',
    'Marketers',
    'Other Offices',
    'Civil DRPs',
    'Regulatory DRPs',
    'Private Residence',
    'Public Office',
    'Administrators'
        ]

def siblingtext(tag):
    return cleantag(tag.find_next_sibling())

def parse_regexmap(text, regexmap):
    data = {}
    for k, v in regexmap.items():
        __ = v(text)
        if __:
            res = __.group(1)
        else:
            res = None
        data.update({k : res})
    return data

class PredictiveOpsBrowser(HomeBrowser):
    def __init__(self, starturl = 'https://predictiveops.com'):
        super(PredictiveOpsBrowser, self).__init__(starturl = starturl)

    def adviserurl(self, crd):
        return self.build_link(r'/advisers/{}'.format(crd))

    def fundurl(self, linktag):
        return self.build_link(linktag['href'])

class AdviserPage(Company):
    def __init__(self, crd, br):
        super(AdviserPage, self).__init__(crd)
        self.br = br
        self.br.open(br.adviserurl(crd))

    @classmethod
    def getdata(cls, crd, br):
        obj = cls(crd, br)
        funds = []
        __ = {
            'crd' : crd,
            'description' : obj.firmdescription,
            'info' : obj.getsubtopics(),
            'people' : obj.get_controlpersons(),
            'relyingadvisers' : obj.relyingadvisers,
                }

        linktags = obj.fundlinks
        if not linktags:
            return __

        for linktag in linktags:
            funds.append(obj.getfundinfo(linktag))
        return mergedicts(__, funds = funds)

    @property
    def soup(self):
        return self.br.soup

    @property
    def firmdescription(self):
        title = self.soup.find('h4',
            text = re.compile('firm overview', re.I))
        return re_DESCRIPTION.sub(r'\1, based in \2',
            siblingtext(title))

    @property
    def fundlinks(self):
        return self.soup.find_all('a',
            attrs = {'href' : re.compile(r'Funds')})
    
    @property
    def relyingadvisers(self):
        return cleantags(self.soup.find_all(lambda tag: tag.name == 'p'
            and tag.find_previous('h4').text == "Relying Advisers"))

    @property
    def subtopics(self):
        _ = defaultdict(list)
        subtopics = self.soup.find_all(
            lambda tag: tag.name == 'p' 
                and tag.get('class') == ['bolder-text', 'mb0'])
        for i in subtopics:
            topic = re.sub(r'(^.*?)(?:\s+\(\d+\))',
                r'\1', i.find_previous('h4').text)
            if topic in TOPICS:
                _[topic.replace(' ', '_').lower()].append(i)
        return _

    def getdrp(self, plaintiff):
        return mergedicts(
            parse_regexmap(siblingtext(plaintiff), re_DRPDICT),
            plaintiff = plaintiff_search(cleantag(plaintiff)).group(1)
                )

    def getlocation(self, location):
        _ = siblingtext(location).replace('\n', ' ')
        return mergedicts(address_search(_).groupdict(),
            {'business' : cleantag(location)})

    def getbusinessinfo(self, topic, tag):
        return {topic.rstrip('s') : cleantag(tag), 'info' : siblingtext(tag)}

    def getfundinfo(self, linktag):
        _ = linktag.find_next('p').text.strip()
        self.br.open(br.fundurl(linktag))
        __ = mergedicts(self.getsubtopics(),
            id_search(self.br.fundurl(linktag)).groupdict(),
            fundinfo = parse_regexmap(_, re_FUNDINFODICT))
        pause(123, 456)
        return __

    def getsubtopics(self):
        data = defaultdict(list)
        for topic, tags in self.subtopics.items():
            for tag in tags:
                _ = plaintiff_search(tag.text)
                if _:
                    data[topic].append(self.getdrp(tag))
                elif location_topic(topic):
                    data[topic].append(self.getlocation(tag))
                else:
                    data['businesses'].append(self.getbusinessinfo(topic, tag))
        return data

    def get_controlpersons(self):
        title = self.soup.find('h4', text = re.compile('^Direct Owners'))
        names = [tag for tag in title.find_next_siblings()
                 if tag.attrs.get('class') == ["bolder-text", "mb0"]
                 and 'Indirect' not in tag.find_previous('h4').text]
        
        owners = []
        for i, name in enumerate(names, 1):
            tag = name
            group = []
            while True:
                if not tag:
                    break
                tag = tag.find_next_sibling()
                if i < len(names):
                    if tag == names[i]:
                        break
                group.append(tag)

            data = {}
            data['controlperson'] = False
            data['name'] = cleantag(name)
            for tag2 in (g for g in group if g):
                if tag2.attrs.get('class') == ["bolder-text", "mb0", "cp-summary"]:
                    data['controlperson'] = True
                else:
                    searched = directowner_search(tag2.text)
                    if searched:
                        data.update(searched.groupdict())
            owners.append(data)
        return owners

br = PredictiveOpsBrowser()
#put it all together
self = AdviserPage(130373, br)
#AdviserPage.getdata(130373, br)
#self = AdviserPage(127831, br)
