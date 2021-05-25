# twitter_harvester

#Running scripts

In config.py - Set Twitter API leys as own personal Developer set and to desired CouchDB connection details for storing data

Data can then be collected from twitter:
- streamer.py can be run to collect real time Tweets
- case_scenarios.py can be run to extract tweets from Australian politician's timelines


Alternatively- file_processor.py for processing JSON files from Twitter corpus on NeCTAR (does not require Twitter API).
This is set up to process a folder of JSON files, set path to FOLDER in file_harvester.py



