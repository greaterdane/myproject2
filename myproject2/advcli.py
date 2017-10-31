import logging
from stagelib import logging_setup
from stagelib.app import DataSlayer, Command
from adviserinfo2 import *
from predictiveops import PredictiveOpsBrowser, AdviserPage, pause

class Process(Command):
    @staticmethod 
    def add_switches(sub_parser): # add arguments here.
        sub_parser.add_argument('--start', help='FormADV entry to start at.', type = int, default = 1)

    def execute(self):
        processfiles(start = self.args.start)

class Scrape(Command):

    @staticmethod 
    def add_switches(sub_parser): # add arguments here.
        sub_parser.add_argument('name', help='Name of scraper to use.', choices = ['predictive_ops', 'brochure'])

    def execute(self):
        scraper = self.args.name
        crdlist = pd.read_csv('lists/predictive_ops_crdlist.csv',squeeze = True)

        if scraper == 'predictive_ops':
            br = PredictiveOpsBrowser()
            logger = logging_setup(name = 'predictive_ops', logdir = 'logs')
            count = 0
            for crd in crdlist.values:
                if br.throttled:
                    logger.warning("Ok, I'm done for now.  Stopped at '{}'".format(crd))
                    break
                while True:
                    retries = 0
                    try:
                        data = AdviserPage.getdata(crd, br)
                        if data:
                            logger.info("Data found for '{}'".format(crd))
                            logger.info("{} private funds listed for {}".format(len(data.get('funds', [])), crd))
                            print "\n\n{}\n\n".format(data['description'])
                            outfile = mkpath(companyfolder(crd), 'predictiveops.json')
                            to_json(outfile, data)
                            logger.info("Data written to {}".format(outfile))
                            count += 1
                            logger.info("{} total listing(s) found.".format(count))
                            pause(214, 1459)
                            break
                        else:
                            break

                    except AttributeError as e:
                        logger.error("'{}' does not have a listing, moving on".format(crd))
                        logger.error(e)
                        break

                    except Exception as e:
                        logger.error("Failed at url '{}' for '{}'".format(br.currenturl, crd))
                        logger.error(e)
                        if br.throttled:
                            logger.error("They are on to us.  We have been throttled.  Let's chill for a bit.")
                            if retries == 2:
                                break
                            retries += 1
                            pause(5239, 11467)
                            continue
                        else:
                            break

            logger.info("{} total listing(s) found.".format(count))    

        elif scraper == 'brochure':
            print "Brochure scraper has not been setup.  Goodbye."

class SpreadSheet(Command):
    pass

if __name__ == '__main__':

    sub_command_map = {
        'processadvs': {'class': Process, 'desc': 'Clean all FormADV spreadsheets and write to "preprocessed" directory.'},
        'scrape': {'class': Scrape, 'desc': 'Scrape the internet for various IAPD related info.  This is no life to live :(.'},
        'spreadsheet': {'class': SpreadSheet, 'desc': 'Create an organized, human-readable spreadsheet for sales and marketing purposes.'},
            }

    DataSlayer.start(sub_command_map)
