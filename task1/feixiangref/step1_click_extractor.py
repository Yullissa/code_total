#!/usr/bin/python

import sys
import json

for line in sys.stdin:
    line = line.strip()
    data = json.loads(line)

    appid = data.get('appid')
    if appid != "oppobrowser":
        continue
   
    docid, event, pos = '', '', ''

    userid = data.get('userid')
    if userid is None:
        continue

    type = data.get('type')
    if type != 'views':
        click = data.get('click')
        if click is None:
            continue

        docid = click.get('docid')
        if docid is None:
            continue
        
        event = click.get('event')
        if event is None:
            continue

        dwell = None
        if event == 'clickDoc' or event == 'play':
            details = click.get('details')
            if details is not None:
                dwell = details.get('dwell')
            if dwell is not None:
                event += ':' + dwell
            else:
                continue

        view = data.get('view')
        if view is not None:
            channelType = view.get('channelType')
            if channelType != "selected":
                continue

            view_details = view.get('details')
            if view_details is not None:
                mashtype = view_details.get('mashtype')
                if mashtype != 'profile':
                    continue

                pos = view_details.get('pos')
                if pos is None:
                    continue

                result = '\t'.join([userid, docid, event, pos])
                print result.encode('utf-8')