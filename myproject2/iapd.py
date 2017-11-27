from db import *
from stage import *
from stagelib.files import fileunzip
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
    unzipped = newfolder(zipfolder, 'unzipped')
    download_formadvs()
    FormADV.tryinsert(list_formadvs())
    for row in FormADV.select():
        fileunzip(row.filename, outdir = ospath.dirname(row.unzipped))
        if row.id < start:
            continue
        FormadvStage.processfile(row.unzipped, formadv_id = row, outfile = row.outfile)
