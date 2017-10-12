import os
from random import randint
from collections import defaultdict
from functools import partial, wraps
import logging
import logging.handlers
import mechanize
from urllib2 import URLError
import numpy as np
import pandas as pd

from stagelib.generic import GenericBase, chunker, to_single_space
from stagelib.fileio import ospath, File, Folder, ospath_decorator, from_json, mkpath, mkdir
from stagelib.web import *
from stagelib.db import *
from stagelib.table import *
from stagelib.fuzzy import results_to_csv

import settings as adv

class IapdDB(Database):
    _udeftables = adv.DBTABLES
    def __init__(self, *args, **kwds):
        super(IapdDB, self).__init__(login = adv.LOGIN, *args, **kwds)
        self.desctables = ['compensation', 'disclosures', 'pct_aum', 'client_types']
        map(self._create_desctable, self.desctables)
        self.desctable = self.descriptions()
    
    @property
    def description_map(self):
        return self.desctable.records.get_mapper('desc', 'id')

    def _create_desctable(self, name):
        return self.create_table(name, ['crd', 'desc', 'value', 'formadv_id'])

    @DatabaseTable.instance
    def advfiling(self, **kwds):
        return 'advfiling'
        
    @DatabaseTable.instance
    def formadv(self, **kwds):
        return 'formadv'

    @DatabaseTable.instance
    def descriptions(self):
        return 'descriptions'

