#
# Tabulator module
#
# Keep running totals/averages over a either
# - a given number of past records, e.g. last 50.
# - a given (past) time period of records, e.g. last 5 mins
#

from collections import defaultdict, Counter

import json, datetime, re

EXTRACT_WORDS = re.compile(r'\b(\w+)\b')

#do_not_include = set(map(lambda s : unicode(s), EXTRACT_WORDS.findall(open("exclude-words.txt").read())))
do_not_include = set(EXTRACT_WORDS.findall(open("exclude-words.txt").read()))
#print "Excluding", len(do_not_include)
#print do_not_include

def extract_words(s):
    words = EXTRACT_WORDS.findall(s)
    return [w.lower() for w in words if len(w) > 2 and w.lower() not in do_not_include]

class WordTabulator():
    def __init__(self, recordkeylist, splitwords = True):
        self.recordkeylist = recordkeylist
        self.splitwords = splitwords
        self.name = "_".join(self.recordkeylist)
        self.freq = Counter()

    def tabulate(self, record):
        for key in self.recordkeylist:
            if key in record:
                if self.splitwords:
                    for w in record[key]:
                        self.freq.update(extract_words(w))
                else:
                    self.freq.update(map(lambda s : s.lower(), record[key]))

    def untabulate(self, record):
        for key in self.recordkeylist:
            if key in record:
                if self.splitwords:
                    for w in record[key]:
                        self.freq.subtract(extract_words(w))
                else:
                    self.freq.subtract(map(lambda s: s.lower(), record[key]))

        # must remove items with zero count
        zerokeys = [key for key in self.freq.keys() if self.freq[key] == 0]
        for zkey in zerokeys:
            del self.freq[zkey]

    def getjsondir(self, howmany = 100):
        mostcommon = self.freq.most_common(howmany)
        return { self.name : mostcommon }

    def filename(self):
        return self.name

class KeyTabulator():
    def getjsondir(self):
        return { self.recordkey : self.keys }

    def __init__(self, recordkey):
        self.keys = defaultdict(int)
        self.recordkey = recordkey

    def tabulate(self, record):
        if record.has_key(self.recordkey):
            for value in record[self.recordkey]:
                self.keys[value] += 1

    def untabulate(self, record):
        if record.has_key(self.recordkey):
            for value in record[self.recordkey]:
                self.keys[value] -= 1
                if self.keys[value] == 0:
                    del self.keys[value]

    def filename(self):
        return self.recordkey

class TimePeriod:
    def __init__(self, length, tabulator):
        self.length = length
        self.tabulator = tabulator
        self.earliest = None
        self.filenamesuffix = "%d" % self.length.total_seconds()

    def appendRecord(self, timeindex, record):
        """
        Return index of earliest element
        """
        self.tabulator.tabulate(record)
        if self.earliest == None:
            self.earliest = timeindex

    def expireRecord(self, record):
        #print timestamp, "expiring", self.length
        self.tabulator.untabulate(record)

    def outputintodir(self, timestamp, dirname):
        filename = "%s.%s" % (self.tabulator.filename(), self.filenamesuffix)
        ofile = open(dirname + "/" + filename, "w")
        dthandler = lambda obj: obj.isoformat() if isinstance(obj, datetime.datetime) else None
        # add timestamp
        o = self.tabulator.getjsondir()
        o['timestamp'] = timestamp
        ofile.write(json.dumps(o, default=dthandler))
        ofile.close()

    def processRecord(self, timestamp, record, clicks, lookuprecord, dirname):
        """
        Process a new record, expires 1 old record (if needed), and output results
        """
        mostrecentindex = len(clicks) - 1
        self.appendRecord(mostrecentindex, record)

        # expire records
        while self.earliest < mostrecentindex:
            oldts, oldid = clicks[self.earliest]
            if oldts < timestamp - self.length:
                oldrecord = lookuprecord(oldid)
                if oldrecord:
                    self.expireRecord(oldrecord)
                self.earliest += 1
            else:
                break

        self.outputintodir(timestamp, dirname)

class EventPeriod(TimePeriod):
    def __init__(self, numberofevents, tabulator):
        self.length = numberofevents
        self.tabulator = tabulator
        self.earliest = None
        self.filenamesuffix = "last%d" % numberofevents

    def processRecord(self, timestamp, record, clicks, lookuprecord, dirname):
        """
        Process a new record, expires old records, and output results
        """
        mostrecentindex = len(clicks) - 1
        self.appendRecord(mostrecentindex, record)

        # expire oldest record
        if mostrecentindex >= self.length:
            oldts, oldid = clicks[mostrecentindex - self.length]
            oldrecord = lookuprecord(oldid)
            if oldrecord:
                self.expireRecord(oldrecord)

        self.outputintodir(timestamp, dirname)
