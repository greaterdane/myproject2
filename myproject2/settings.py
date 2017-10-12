from stagelib.fileio import ospath, mkdir

BASEDIR = ospath.dirname(ospath.abspath(__file__))
DATADIR = mkdir(BASEDIR, 'data')
CONFIGDIR = mkdir(BASEDIR, 'config')
CACHEDIR = mkdir(DATADIR, 'datacache')

LOGIN = {
    "user": "josh",
    "passwd": "josh",
    "db": "advisor_info2",
    "host":"192.168.0.16",
    "port": 3306,
    "local_infile" : 1,
    "charset" : 'utf8',
    "use_unicode" : True,
            }

DBTABLES = {
    'formadv' : {
        'fields' : [
            ('filingdate', "DATE",),
            ('filename', "VARCHAR(255)",),
            ('output', "VARCHAR(255)",),
            ('ext', "VARCHAR(6)",),
            ('rows_original', "INT(11) DEFAULT NULL",),
            ('rows_imported', "INT(11) DEFAULT NULL",),
            ('date_created', "TIMESTAMP NOT NULL",),
                ],
        'constraints' : ['filingdate', 'filename']
            },

    'competitors' : {
        'fields' : [('name', "VARCHAR(150)")],
        'constraints' : ['name']
        }
    }