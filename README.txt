Hi, my name is Tristan. I'm sure that you're busy people/wary of 3rd party scripts so thank you for even going so far as to read this!

We are hoping to use your superior downloading infrastructure to augment our own ability to retreive a sizable amount of data. In terms of raw bytes, this total dataset is not particularly large at all. However, when one considers the temperamentality of the server it's on, combined with the fact that said server seems to only allow a single download at a time per connected computer, the effective sizwe of the dataset grows considerably.

What we would like to do is maximize the number of computers downloading from the server at any one time. Each computer will only download a single file at a time, but having so many computer downloading in parallel should dramatically decrease the total download time. There is a very real possibility that the server in question will become overwhelmed and shut down all connections. If that occurs, then we have no choice but to download all of the files from our own system here in Juneau, which we are already in the process of doing, so no work is lost.

I have structured this script to work with an arbitrary number of computers, with just a little human powered setup. All of the download urls that we are trying to download are contained within 155 text files (which in turn are in the all_urls.zip file that shares a directory with this README). Each of these files contains 1000 download urls. When you look inside the script folder, you will see two python scripts, and two subdirectories. The 'urls' subdirectory is a lot like the magazine on a gun. Any of the url text files that are placed in that subdirectory will have their urls read out and downloaded by the script when it is executed. So, simply place a single copy of the 'mass_download' directory on each computer that will be participating. Then, for each computer, simply place a proportional number of the url text files inside of the url directory, and execute it. Make sure that each computer has a different set of url text files, so as to avoid duplicate downloading. A little duplication is not a problem, but it defeats the purpose of spreading out the load if there's too much.

To execute the script, just execute downloadWrapper.py as a python script.

The script is fairly robust, so once you execute it you can just forget about it. It will probably take about a week for the script to complete. If you'd like to see what it's doing, just set the 'verbose' values in both scripts to True. Otherwise, it will run silently until it completes, at which point it will print a message to the console saying that it has finished. All downloaded files will be in the 'downloaded' subdirectory.

Please don't hesitate to contact me if you need clarification/help. 

Tristan Sebens
tristan.ng.sebens@gmail.com
1-(907)-500-5340