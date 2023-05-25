
### Demo Fetcher
Launched by 'run.sh' from the root of this project directory. 
setting the date, num_procs, and sample_size in that .conf file will change the run behavior.
Leave sample_size unset to run on an entire day's RSS content. 

This fetcher works by launching several worker processes. One of them spins up, downloads the RSS content, and splits it into batches- the rest wait until that task is complete, then all work to consume those batches. Html and http metadata is written to disk, and the http metadata is sent to the rabbit-mq queue as defined by the pipeline construction. Right now we just print the results of that pipeline when it finishes. 

Logging and Monitoring instrumentation is more or less absent here. 

Final implimentation will probably split the rss-download and batching operations into a separate worker from the actual fetchers, instead of this approach. 
