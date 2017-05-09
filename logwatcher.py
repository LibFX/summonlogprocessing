#!/usr/bin/python -u
#
#
# @author Godmar Back godmar@gmail.com 2013
#
# Watch the Summon click.log and look up metadata info about 
# things users clicked on, and compute summary statistics ove
# past time periods.
#
# We store the metadata of each clicked-on item in a ZODB
# database.  We assume we can keep a log of all clicks in 
# memory, though not necessarily the actual records.
#

import pyinotify, summon, sys, re, time, datetime, os, threading, json
import glob
import getopt
import gzip

def usage(p):
    print "Usage: %s [-h] [-m] [-l dir] [-n nrecords] file1 .. fileN" % (p)
    print " -h:     Print this help"
    print " -s:     Just obtain records from Summon in bulk, do not write JSON files"
    print " -m:     Don't exit; instead, monitor fileN for appended log lines"
    print " -l:     Directory in which to write live summaries"
    print " -n:     Number of past records to store, default = 50"

try:
    opts, args = getopt.getopt(sys.argv[1:], "hmsl:n:")
except getopt.GetoptError as err:
    print str(err)
    usage(sys.argv[0])
    sys.exit(2)

opts = dict(opts)
bulkdownload = opts.has_key('-s')

if opts.has_key('-h') or len(sys.argv) < 2:
    usage(sys.argv[0])
    sys.exit(2)

#logfilename = '/var/log/apache2/click.log'
logfilename = sys.argv[1]
dbfilename = 'summonclicks.db'

if opts.has_key('-n'):
    livesummarymax = int(opts['-n'])
else:
    livesummarymax = 50

if opts.has_key('-l'):
    livesummarydir = opts['-l'] + "/%s"
else:
    livesummarydir = "livesummaries/%s"

# 98.229.50.89 - - [13/Jan/2013:21:46:02 -0500] "GET /services/summonlogging/click.gif?id=FETCH-crossref_primary_10_1080_00139157_1983_99298521&_ts=319200 HTTP/1.1" 200 333
regex = re.compile(r'\S+ - - \[(\S+) (\S+)\] \"GET /services/summonlogging/click\.gif\?id=(\S*?)&(bookmark=(\S*?))?&')

# http://www.ibm.com/developerworks/web/library/wa-apachelogs/
# Apache's date/time format is very messy, so dealing with it is messy
# This class provides support for managing timezones in the Apache time field
# Reuses some code from: http://seehuhn.de/blog/52
class timezone(datetime.tzinfo):
    def __init__(self, name="+0000"):
        self.name = name
        seconds = int(name[:-2])*3600+int(name[-2:])*60
        self.offset = datetime.timedelta(seconds=seconds)

    def utcoffset(self, dt):
        return self.offset

    def dst(self, dt):
        return timedelta(0)

    def tzname(self, dt):
        return self.name

def parse_apache_date(date_str, tz_str):
    '''
    Parse the timestamp from the Apache log file, and return a datetime object
    '''
    tt = time.strptime(date_str, "%d/%b/%Y:%H:%M:%S")
    tt = tt[:6] + (0, timezone(tz_str))
    return datetime.datetime(*tt)

import sqlite3
conn = sqlite3.connect(dbfilename)
conn.execute('''CREATE TABLE IF NOT EXISTS summonrecords
                (id TEXT PRIMARY KEY, recordjson TEXT) 
             ''')
   
def insertrecord(conn, id, record):
    conn.execute("INSERT OR REPLACE INTO summonrecords VALUES (?, ?)", (id, json.dumps(record)))
    conn.commit()
    
def lookuprecord(conn, id): 
    cursor = conn.cursor()
    cursor.execute("SELECT recordjson FROM summonrecords WHERE id = ?", (id,)) 
    r = cursor.fetchone()
    if r:
        r = json.loads(r[0])

    cursor.close()
    return r

from tabulator import EventPeriod, TimePeriod, KeyTabulator, WordTabulator
#
#
#
timeperiods = [ datetime.timedelta(minutes = 1), 
                datetime.timedelta(minutes = 5),
                datetime.timedelta(hours = 1),
                datetime.timedelta(hours = 24),
                datetime.timedelta(days = 7) ]
eventperiods = [ 2000, 1000, 500, 200, 100, 50 ]

# for others, see
# http://stackoverflow.com/questions/546321/how-do-i-calculate-the-date-six-months-from-the-current-date-using-the-datetime

clicks = []
periods = []
livesummaryindex = 0

keyList = ['Discipline', 'ContentType', 'SourceType', 'PublicationYear']
wordList1 = [['Abstract'], ['Title'], ['Abstract', 'Title']]
wordList2 = [['Keywords'], ['SubjectTerms'], ['Keywords', 'SubjectTerms']]

for tp in timeperiods:
    for k in keyList:
        periods.append(TimePeriod(tp, KeyTabulator(k)))

    for k in wordList1:
        periods.append(TimePeriod(tp, WordTabulator(k, True)))

    for k in wordList2:
        periods.append(TimePeriod(tp, WordTabulator(k, False)))

for tp in eventperiods:
    for k in keyList:
        periods.append(EventPeriod(tp, KeyTabulator(k)))

    for k in wordList1:
        periods.append(EventPeriod(tp, WordTabulator(k, True)))

    for k in wordList2:
        periods.append(EventPeriod(tp, WordTabulator(k, False)))

def fetchrecord(bookmark, expectedid):
    #query = { 's.q' : '(id:' + id + ')' }
    #query = { 's.fids' : id }
    query = { 's.bookMark' : bookmark }
    #query = { 's.q' : '(id:' + id + ')' }
    try:
        result = summon.search(query)
        # FETCH-gale_primary_2979500031 has recordCount == 1 and no documents
        if result.has_key('recordCount') and result['recordCount'] == 1 and len(result['documents']) > 0:
            record = result['documents'][0]
            id = record['ID'][0]
            insertrecord(conn, id, record)
            if id != expectedid:
                insertrecord(conn, expectedid, record)

            return record
        else:
            print bookmark, "not found in Summon"
            return False
    except Exception as err:
        print bookmark, "caused error when searching in Summon", err
        return False


def chunks(l, n):
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def downloadbulk(missingids):
    total = 0
    print "Attempting to bulk download", len(missingids), "records"
    for chunk in chunks(missingids, 100):
        query = { 's.fids' : ",".join(chunk) }
        try:
            #print query
            result = summon.search(query)
            # FETCH-gale_primary_2979500031 has recordCount == 1 and no documents
            if result.has_key('documents') and len(result['documents']) > 0:
                total += len(result['documents'])
                print "Retrieved",total
                for record in result['documents']:
                    id = record['ID'][0]
                    insertrecord(conn, id, record)
            else:
                print "No results found in Summon"
        except Exception as err:
            print "Caused error when searching in Summon", err


missingids = []

def processlogline(logfile):
    global livesummaryindex
    l = logfile.readline() 
    # current end of file
    if l == "":
        return False
    m = regex.match(l)
    # ignore lines that don't match
    if not m:
        return True

    id = m.group(3)
    timestamp = parse_apache_date(m.group(1), m.group(2))
    mostrecentindex = len(clicks)
    for lts, lid in clicks[-10:]:
        if lid == id:
            print timestamp, "repeat, ignoring"
            return True

    print timestamp,
    record = lookuprecord(conn, id)
    if record:
        print 'Hit', id
    else:
        if bulkdownload:
            missingids.append(id)
        else:
            bookmark = m.group(5)
            record = fetchrecord(bookmark, id)
            print 'Miss', id,
            if record == False:
                print "... fetch failed"
                return True
            else:
                print "... found"

    if bulkdownload:
        return True

    clicks.append((timestamp, id))

    # output record and new summary 
    # wipe directory
    dirname = livesummarydir % (str(livesummaryindex))
    if not os.access(dirname, os.F_OK):
        os.mkdir(dirname)

    for filename in glob.glob(dirname + "/*"):
        os.remove(filename)

    recordfile = open(dirname + "/Record", "w")
    dthandler = lambda obj: obj.isoformat() if isinstance(obj, datetime.datetime) else None
    recordfile.write(json.dumps({ 'timestamp' : timestamp, 'record' : record }, default=dthandler))
    recordfile.close()

    def findrecord(id):
        return lookuprecord(conn, id)

    for period in periods:
        period.processRecord(timestamp, record, clicks, findrecord, dirname)

    # print "linking", dirname
    symlinkname = livesummarydir % ("now")
    if os.access(symlinkname, os.F_OK):
        os.unlink(symlinkname)
    os.symlink(str(livesummaryindex), symlinkname)
    livesummaryindex = (livesummaryindex + 1) % livesummarymax
    return True

def log(msg):
    print datetime.datetime.now(), msg

def processexistinglines(logfilename):
    log("opening logfile %s" % (logfilename,))
    if logfilename.endswith(".gz"):
        logfile = gzip.open(logfilename, "rb")
    else:
        logfile = open(logfilename)

    # process existing lines
    while processlogline(logfile):
        True

    return logfile

# process provided files
lastlogfile = None
for logfilename in args:
    if lastlogfile:
        lastlogfile.close()

    lastlogfile = processexistinglines(logfilename)

if bulkdownload:
    missingids = list(set(missingids))
    for i in range(1, 40):
        print "Attempt #", i, len(missingids), "left"
        downloadbulk(missingids)
        missingids = [id for id in missingids if not lookuprecord(conn, id)]

    sys.exit(0)

# if not watching for modifications, stop
if not opts.has_key('-m'):
    sys.exit(0)

lastlogfilename = args[-1]

# process any modifications
wm = pyinotify.WatchManager()

keepgoing = [ True ]

class FileWatcher(pyinotify.ProcessEvent):
    def __init__(self, lastlogfilename, lastlogfile):
        self.lastlogfilename = lastlogfilename
        self.lastlogfile = lastlogfile
        self.wdd = wm.add_watch(self.lastlogfilename, pyinotify.IN_MODIFY)

    def process_IN_MODIFY(self, event):
        while processlogline(self.lastlogfile):
            True

    def process_IN_CREATE(self, event):
        if os.path.basename(event.pathname) == "stop":
            log("detected stop file, stopping")
            os.unlink(event.pathname) 
            keepgoing[0] = False 

        if os.path.basename(event.pathname) == os.path.basename(self.lastlogfilename):
            log("detected log rotation, reopening %s" % self.lastlogfilename)
            wm.rm_watch(self.wdd.values())
            self.lastlogfile.close()
            self.lastlogfile = open(self.lastlogfilename)
            self.wdd = wm.add_watch(self.lastlogfilename, pyinotify.IN_MODIFY)

notifier = pyinotify.Notifier(wm, FileWatcher(lastlogfilename, lastlogfile))

logdirtowatch = os.path.dirname(lastlogfilename)
logdirtowatch = "." if logdirtowatch == "" else logdirtowatch

# watch current directory - we 'touch stop' to stop this process
log("watching directory %s for log file rotation" % logdirtowatch)
wdd = wm.add_watch(logdirtowatch, pyinotify.IN_CREATE)

while keepgoing[0]:
    try:
        # process the queue of events as explained above
        notifier.process_events()
        if notifier.check_events():
            # read notified events and enqeue them
            notifier.read_events()
    except KeyboardInterrupt:
        break

notifier.stop()
conn.close()    # close db
