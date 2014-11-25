#!/usr/bin/python
"""
  A script to parse sample log.
  [log_format]    $remote_addr - $remote_user [$time_local] 
                  "$request" $status $body_bytes_sent 
                  "$http_referer" "$http_user_agent" [unknown_content]; 
  [output_format] ip,user,time,request,status,size,referer,agent
  
"""
import sys
import re
import time

COL_DELIMITER = '\x01';

def convertTime(str):
        # convert: 12/Feb/2014:03:17:50 +0800
        #      to: 2014-02-12 03:17:50
        # [note] : timezone is not considered
    if str: 
        return time.strftime('%Y-%m-%d %H:%M:%S', 
                time.strptime(str[:-6], '%d/%b/%Y:%H:%M:%S'))

def parseLog(inFile, outFile, dirtyFile):
    file   = open(inFile)
    output = open(outFile, "w")
    dirty  = open(dirtyFile, "w")
    items  = [
        r'(?P<ip>\S+)',                     # ip 
        r'\S+',                             # indent -, not used 
        r'(?P<user>\S+)',                   # user 
        r'\[(?P<time>.+)\]',                # time
        r'"(?P<request>.*)"',               # request
        r'(?P<status>[0-9]+)',              # status
        r'(?P<size>[0-9-]+)',               # size
        r'"(?P<referer>.*)"',               # referer
        r'"(?P<agent>.*)"',                 # user agent
        r'(.*)',                            # unknown info
    ]
    pattern = re.compile(r'\s+'.join(items)+r'\s*\Z')
    for line in file:
        m = pattern.match(line)
        if not m:
            dirty.write(line)
        else:
            dict = m.groupdict()
            dict["time"] = convertTime(dict["time"])

            if dict["size"] == "-":
                dict["size"] = "0"

            for key in dict:
                if dict[key]=="-":
                    dict[key] = ""            
            #ip,user,time,request,status,size,referer
            output.write("%s\n" % (COL_DELIMITER.join(
                    (dict["ip"],
                      dict["user"],
                      dict["time"],
                      dict["request"],
                      dict["status"],
                      dict["size"],
                      dict["referer"], 
                      dict["agent"] ))))

    output.close()
    dirty.close()
    file.close()

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print "Usage: %s <input_file> <output_file> <dirty_file>" % sys.argv[0]
        sys.exit(1)
    parseLog(sys.argv[1], sys.argv[2], sys.argv[3])
