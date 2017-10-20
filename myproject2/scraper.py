from stagelib import configurelogging
from adviserinfo import *
    
class IapdBrowser(HomeBrowser):
    def __init__(self, starturl = 'https://www.adviserinfo.sec.gov'):
        super(IapdBrowser, self).__init__(starturl = starturl)

    def noresults(self):
        return self.check_currenturl('SearchNoResult.aspx$')

    def getbrochures(self, crd):
        __ = InvestmentAdviser(crd)
        self.follow_link(text = "Part 2 Brochures")
        linktags = list(self.filterlinks('BRCHR_VRSN'))
        for tag in linktags:
            link = self.buildlink(tag.url)
            filename = "%s.pdf" % to_single_space(tag.text.translate(None, '\"/-*?|<>:[]'))
            self.download(link, mkpath(__.brochuredir, filename))
            self._logger.info("Sucessfully downloaded '%s' to '%s'" % (filename, __.brochuredir), extra = {'crd' : crd})
            pause(636, 838)

    def adviserurl(self, crd):
        return self.buildlink('/IAPD/IAPDFirmSummary.aspx?ORG_PK=%s' % crd)

    def browse(self, crd, getitems = False): #NOT FINISHED
        url = adviserurl(crd)
        self.open(url)
        if self.noresults():
            raise Exception("No search results. Try https://brokercheck.finra.org/firm/summary/%s" % crd)
        self.getbrochures(crd)
        self.back()

if __name__ == '__main__':
    self = IapdBrowser()
    self._logger = configurelogging(logging.getLogger(), extrakeys = ['crd'])
    browsed = set(pd.read_csv('adviserinfo.log',
        delimiter = '|',
        header = None,
        dtype = object)[0].tolist())

    crd_iter = [(i.strip('"\n') for i in cg
        if not i in browsed) for
            cg in File(r'lists\crdlist.csv',
                chunksize = 120)]

    for crd_group in crd_iter:
        for crd in crd_group:
            #this try block should be a decorator
            try:
                self.browse(crd)
                self._logger.info("Sucessfully Browsed.", extra = {'crd' : crd})
            except (mechanize._mechanize.LinkNotFoundError) as e:
                self._logger.error("LinkNotFoundError", extra = {'crd' : crd})
            except URLError as e:
                self._logger.error(("URLError", e), extra = {'crd' : crd})
            except Exception as e:
                if self.throttled:
                    self._logger.error((e, "Bad Status code: %s" % self.statuscode), extra = {'crd' : crd})
                self._logger.error(e, extra = {'crd' : crd})
            pause(236, 396)
        pause(4007, 5696)