# twitter_harvester

#Running scripts

In config.py - Set Twitter API keys as own personal Developer set and to desired CouchDB connection details for storing data

Data can then be collected from twitter:
- streamer.py can be run to collect real time Tweets
- case_scenarios.py can be run to extract tweets from Australian politician's timelines


Alternatively- file_harvester.py for processing JSON files from Twitter corpus on NeCTAR (does not require Twitter API).
Set FOLDER in file_harvester.py to path containing JSON files you want to process and add to db.



