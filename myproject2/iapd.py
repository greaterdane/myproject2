from db import *
from stage import *
from stagelib.files import ospath, fileunzip
from scraper import download_formadvs
from stagelib.cli import Processor, Normalize

def get_filingdate(path):
    return pd.to_datetime(re.sub(r'^.*?ia(\d+)\.zip', r'\1', path))

def list_formadvs():
    return sorted([
        {'date' : get_filingdate(path), 'filename' : path}
        for path in Folder.listdir(zipfolder, pattern = '.zip$')
            ], key = lambda k: k['date'])

def setup(start = 1):
    download_formadvs()
    FormADV.tryinsert(list_formadvs())
    for row in FormADV.select():
        if not ospath.exists(row.unzippedfile):
            fileunzip(row.filename, row.unzippedfolder)

        if row.id < start:
            continue
        FormadvStage.processfile(row.unzippedfile,
                                 formadv_id = row,
                                 skiprows = 0,
                                 outfile = row.outfile)
