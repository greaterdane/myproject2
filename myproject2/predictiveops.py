import re
from stagelib.web import *

from adviserinfo import Company

re_DIRECTOWNERS = re.compile(r'^\n(?P<role>[A-Z]+.*?) +\(since (?P<since>\d+\/\d+)\)\n+Ownership Percentage: +(?P<ownership>.*?)\n')

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
    def firmdescription(self):
        title = self.br.findtag('h4',
            text = re.compile('firm overview', re.I))
        return clean_tag(title.find_next_sibling())

    @property
    def fundlinks(self):
        return self.soup.find_all('a', attrs = {'href' : re.compile(r'funds')})



#bolder-text mb0 cp-summary

br = PredictiveOpsBrowser()
ap = AdviserPage(108149, br)
direct_owners_title = ap.br.findtag('h4', text = re.compile('Direct Owners'))
direct_owner_names = [tag for tag in direct_owners_title.find_next_siblings()
                      if tag.attrs['class'] == ["bolder-text", "mb0"]]

owners = []
for i, name in enumerate(direct_owner_names, 1):
    tag = name
    group = []
    while True:
        if not tag:
            break
        tag = tag.find_next_sibling()
        if i < len(direct_owner_names):
            if tag == direct_owner_names[i]:
                break
        group.append(tag)
    data = {}
    data['controlperson'] = None
    data['name'] = clean_tag(name)
    for gtag in (g for g in group if g):
        if gtag.attrs['class'] == ["bolder-text", "mb0", "cp-summary"]:
            data['controlperson'] = gtag.text
        else:
            searched = re_DIRECTOWNERS.search(gtag.text)
            if searched:
                data.update(searched.groupdict())
    owners.append(data)
