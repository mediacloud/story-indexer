# Mediacloud Story Pipeline

!! Under Construction !!

Currently contains a common filesystem api, and a demonstration multi-worker html fetcher using that as a backend. 


### Filesystem Api
An API which manages access to a filesystem data store, and also manages pipeline state as it pertains to the filesystem.


### Demo Fetcher
Launched by 'supervisord -c fetch_demo_supervisor.conf'
setting the date, num_procs, and sample_size in that .conf file will change the run behavior.
Leave sample_size unset to run on an entire day's RSS content. 




## No implimentation Yet

### Queueing Architecture / Overall orchestration
...

### Metadata Extraction
...

### Logging Dashboard
...

### Index Injestion
...

