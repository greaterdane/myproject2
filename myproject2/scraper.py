from adviserinfo import *

def get_question(tag):
    return clean_tag(tag.find_previous('td'))

def get_redtext(tag):
    __ = tag.findAll('span', {'class' : ["PrintHistRed"]})
    if not __:
        return tag.findAll('font', {'color' : "#ff0000"})
    return __

def get_checked_boxes(soup):
    return soup.find_all('img', {'alt' : re.compile(r'(?<!not )(?:changed|selected|checked)', re.I)})

def get_form_answers(soup):
    return get_redtext(soup) + get_checked_boxes(soup)
    
def get_QA(soup):
    d = {}
    for tag in get_checked_boxes(soup):
        d.update({clean_tag(tag.find_previous('td', attrs = {'colspan' : re.compile(r'\d+')})) : tag})
    return d

class IapdBrowser(HomeBrowser):
    def __init__(self, starturl = 'https://www.adviserinfo.sec.gov'):
        super(IapdBrowser, self).__init__(starturl = starturl)
        self.no_search_results = partial(self.check_current_url, 'SearchNoResult.aspx$')
        self.throttled = partial(self.check_current_url, '^(?:503|403|401)$')

    def get_brochures(self, crd):
        folder = mkdir(mkpath(adv.DATADIR, crd), 'brochures') ##Need to have a crd object
        self.follow_link(text = "Part 2 Brochures")
        link_tags = list(self.filter_links('BRCHR_VRSN'))
        for tag in link_tags:
            link = self.build_link(tag.url)
            filename = "%s.pdf" % to_single_space(tag.text.translate(None, '\"/-*?|<>:[]'))
            self.download(link, mkpath(folder, filename))
            self.logger.info("Sucessfully downloaded '%s' to '%s'" % (filename, folder), extra = {'crd' : crd})
            pause(636, 838)

    def get_summary_link(self, crd):
        return self.build_link('/IAPD/IAPDFirmSummary.aspx?ORG_PK=%s' % crd)

    def scrape_item(self, itemname):
        data = []
        redtext = []
        try:
            self.follow_link(text = itemname) ##"Item 5 Information About Your Advisory Business"
        except (mechanize._mechanize.LinkNotFoundError) as e:
            self.logger.error("LinkNotFoundError")
        for row in self.soup.findAll('tr'):
            try:
                if get_checked_boxes(row) or get_redtext(row):
                    data.append(row)
                    redtext.append(get_redtext(row))
            except TypeError as e:
                ###Logging
                self.logger.error(e)
                #return
            except Exception as e:
                self.logger.error("PARSING ERROR.")
                self.logger.error(e)
            else:
                return data

    def get_items(self): #NOT FINISHED
        # path = str(self.cdir.joinpath("first.html"))
        # write_soup_local(self.soup, path)

        ##from inital summary page
        self.follow_link(text = "View Form ADV By Section")

        for k,v in cnf['sections'].items():
            #break  ##for development only.

            ##can we randomly sort 'v' (the section items) each time ???
            items = {}
            for k2, v2 in tqdm( v.items(), desc = k ):
                data = self.scrape_item(v2)
                if data:
                    items.update({k2 : data})
                self.back()

    def scheduleD(self):
        groups = defaultdict(list)
        tags = get_redtext(self.soup)
        for tag in tags:
            group = tag.find_previous('a', attrs = {'name' : re.compile('^[A-Z]+$')})
            if group:
                name = clean_tag(group)
                if not re.search(r'^\d+|^\(', name):
                    question = get_question(tag)
                    if not question:
                        question = "N/A"
                    groups[clean_tag(group)].append({'question' : question, 'answer' : tag.text})
        return groups
        #qa = get_QA(self.soup)

    def browse(self, crd, getitems = False): #NOT FINISHED
        sl = get_summary_link(crd)
        self.open(sl)
        if self.no_search_results():
            raise Exception("No search results. Try https://brokercheck.finra.org/firm/summary/%s" % crd)
        self.get_brochures(crd)
        ##go back in between tasks here and there and do a random pause in between tasks.
        ## other tasks/items here
        # ...
        #self.get_items()
        self.back()

#TODO:
    #classify objects (crd, etc.)
    #db integration
    #items
    #schedule a
    #schedule b
    #schedule d
#DONE
    #part 2 brochures

if __name__ == '__main__':
    self = IapdBrowser()
    self.logger = logging.getLogger()
    self.logger.setLevel(logging.DEBUG)

    if not self.logger.handlers:
        formatter = logging.Formatter("%(crd)s|%(levelname)s|%(asctime)s|%(message)s")
    
        fh = logging.handlers.RotatingFileHandler('adviser_info.log', encoding = 'utf-8')
        fh.setLevel(logging.DEBUG)
        fh.setFormatter(formatter)
        
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)

        self.logger.addHandler(fh)
        self.logger.addHandler(ch)

    browsed = set(pd.read_csv('adviser_info.log',
        delimiter = '|',
        header = None,
        dtype = object)[0].tolist())

    crd_iter = [(i.strip('"\n') for i in cg
        if not i in browsed) for
            cg in File(r'lists\crd_list.csv',
                chunksize = 120)]

    for crd_group in crd_iter:
        for crd in crd_group:
            mkdir(adv.DATADIR, crd)
            try:
                self.browse(crd)
                self.logger.info("Sucessfully Browsed.", extra = {'crd' : crd})
            except (mechanize._mechanize.LinkNotFoundError) as e:
                self.logger.error("LinkNotFoundError", extra = {'crd' : crd})
            except URLError as e:
                self.logger.error(("URLError", e), extra = {'crd' : crd})
            except Exception as e:
                if self.status_code in [503, 403, 401]:
                    self.logger.error((e, "Bad Status code: %s" % self.status_code), extra = {'crd' : crd})
                self.logger.error(e, extra = {'crd' : crd})
            pause(236, 396)
        pause(4007, 5696)