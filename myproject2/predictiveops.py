import re
from stagelib import dictupgrade, mergedicts
from stagelib import isearch
from stagelib.web import *

from adviserinfo import Company

directowner_search = isearch(r'^\n(?P<title>[A-Z]+.*?) +\(since (?P<since>\d+\/\d+)\)\n+Ownership Percentage: +(?P<ownership>.*?)\n')
plaintiff_search = isearch(r'^Plaintiff: +(?P<plaintiff>.*?$)')
address_search = isearch(r'^(?P<address>.*?)\s+Phone:(?P<phone>[^\s]+)\s+Fax:(?P<fax>[^\s]+)')
location_topic = isearch(r'books|other office')

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

TOPICS_OF_INTEREST = [
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
    'Public Office'
        ]

class PredictiveOpsBrowser(HomeBrowser):
    def __init__(self, starturl = 'https://predictiveops.com'):
        super(PredictiveOpsBrowser, self).__init__(starturl = starturl)

    def adviserurl(self, crd):
        return self.build_link(r'/advisers/{}'.format(crd))

class AdviserPage(Company):
    def __init__(self, crd, br):
        super(AdviserPage, self).__init__(crd)
        self.br = br
        self.br.open(br.adviserurl(crd))

    @property
    def soup(self):
        return self.br.soup

    @property
    def firmdescription(self):
        title = self.soup.find('h4',
            text = re.compile('firm overview', re.I))
        return clean_tag(title.find_next_sibling())

    @property
    def fundlinks(self):
        return self.soup.find_all('a',
            attrs = {'href' : re.compile(r'funds')})
    
    @property
    def relyingadvisers(self):
        return self.soup.find_all(lambda tag: tag.name == 'p'
            and tag.find_previous('h4').text == "Relying Advisers")

    @property
    def subtopics(self):
        st = self.soup.find_all(lambda tag: tag.name == 'p'
            and tag.get('class') == ['bolder-text', 'mb0'])
        __ = self.relyingadvisers
        return [i for i in st if i not in __]

    def getdrp(self, topic, plaintiff):
        data = {
            'plaintiff' : plaintiff_search(plaintiff.text).group(1),
            'category' : clean_tag(topic).replace(' ', '_').lower() ##Add this in a decorator, most data seems to follow this pattern (end)
                }
        text = clean_tag(plaintiff.find_next_sibling()) ##Add this in a decorator, most data seems to follow this pattern (beginning)
        for k, v in re_DRPDICT.items():
            __ = v(text)
            if __:
                res = __.group(1)
            else:
                res = None
            data.update({k : res})
        return data

    def get_location(self, topic, location):
        return address_search(location).groupdict()
        
    def get_subtopics(self):
        for subtopic in self.subtopics:
            topic = subtopic.find_previous('h4')
            if topic.text not in TOPICS_OF_INTEREST:
                continue
            ps = plaintiff_search(subtopic.text)
            print topic
            print subtopic
            if ps:
                print self.getdrp(topic, subtopic)
                #yield self.getdrp(topic, subtopic)
            elif location_topic(topic.text):
                print subtopic
                #yield get_location(topic, subtopic)
            #custodians, auditors, prime brokers are just {topic : subtopic}

            #for sibling in subtopic.find_next_siblings():
            #    data = clean_tag(sibling)
            #    text = ''.join(_[:-1])
            #    ting = mergedicts(dictupgrade(self.getdrp, text),
            #        {'plaintiff' : ps.group(1)})

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
            data['name'] = clean_tag(name)
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
#self = AdviserPage(130373, br)
self = AdviserPage(127831, br)
