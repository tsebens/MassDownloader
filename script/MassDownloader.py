"""
Script designed to download large numbers of files one at at time. Script is capable of monitoring download stream to detect dead streams, in which case the file will be deleted and the download restarted.

Script also provides functionality for checking if a file has been completely downloaded, although it does not directly invoke this functionality by default as it can add immense amount of processing time to the total download time.

Script is capable of downloading from a list of urls stored in memory, or a text file.

Author: Tristan Sebens
eMail: tristan.ng.sebens@gmail.com
Phone No: +1-(907)-500-5430
"""

# Mass downloader
import sys

from script.Agent import dlFile

sys.path.append( r"N:\Python Scripts\BathymetryProcessor" )
import urllib
import random
import os.path
import time
from multiprocessing import Process as proc
import traceback

proc_check_time = 10 # The number of seconds the main process will wait between file size checks (which are done to determine download stream activity)
dl_att_thshold = 5 # The number of times the script will attempt to download a file before abandoning it. Set to -1 for infinite attempts.
max_dead_cycles = 4 # The number of cycles the script will wait for the download stream to restart once it has died.
base_wait_time = 2 # This will be multiplied by a random value between .5 and 1.5 between each download to determine the number of seconds the script will wait. Its an attempt to prevent the server from kicking us off.
restart_wait_time = 5 # The number of seconds the script will pause when restarting a download
file_creation_wait_limit = 60 # The number of seconds the script will wait for the file to be created before asumming an error has occurred.
dl_completion_threshold = 1.0 # The percentage of the file which must be downloaded for the file to be considered 'completely' downloaded. Useful if there is a consistant difference between file size once downloaded, even when download is truly complete, or when a file can still be used when <100% complete (CSV files, eg.).
# If set to true, the script will check each file already downloaded for completeness. Can take a long time, because for each file you have to ask the server for the size of the file on their disk, which usually takes a few seconds per request. So for 10,000 files it can take several hours to check
checkDownloadCompleteness = False
verbose = True # Set to true for the script to describe its behaviour in real time via the console


#Not working yet
loop_dl_attempts = True # If true, failed download attempts (after reaching the attempt threshold^^) will be relocated to the back of the list to be tried again later.

class FileNotCreatedException( Exception ):
	pass

class DownloadStreamDeadException( Exception ):
	pass

def printIfVerbose( message ):
	if verbose == True:
		print( message )
	
def findFilesByExtension( ROOT, EXTENSION ):
	AllFiles = os.walk( ROOT, True, None, False )
	ReturnFiles = list()

	for entry in AllFiles:
		# entry is a tuple containing (path, directories, files)
		for file in entry[2]:
			( filename, extension ) = os.path.splitext( file )
			if extension == EXTENSION:
				ReturnFiles.append( os.path.join( entry[0], file ) )

	return ReturnFiles

# This code only works for the autogrid bathymetry urls.
def getNameFromURL( url ):
	basename = os.path.basename( url )
	name = basename.split( '.' )[0]
	return name

# Function used to cleanly restart a dead download stream
def restartDL( p, f_path, url ):
	p.terminate()
	time.sleep( restart_wait_time )
	os.remove( f_path ) # Delete the old file so that the new one doesn't hit it
	p = proc(target=dlFile, args=(url, f_path))
	p.start()
	return p

# The basic download function

def getFileSizeOnServer( url ):
	d = urllib.urlopen( url )
	size = int( d.info()['Content-Length'] )
	urllib.urlcleanup()
	return size
		
# Returns true if the file at fp and the file at url are the same size on disk.
# The percentage of the file that has to be present for it to be considered 'complete' can be altered by changing dl_completion_threshold
def downloadComplete( url, fp ):
	size_on_server = getFileSizeOnServer( url )
	size_on_disk = os.path.getsize( fp )
	if size_on_disk >= size_on_server:
		return True
	return False
	
# You can pass a postfix in through post which will be affixed to the end of the filename, before the file extension. Useful if you're downloading multiple files which all have the same output name (AutoGrid, a website we use a lot, does this), and want to distinguish between them 
def dlFileWithProcChecks( url, f_path, post='' ):
	try:
		# Fist we check if the file already exists.
		if os.path.isfile( f_path ):
			printIfVerbose(  "%s already present on disk." % f_path )
			if checkDownloadCompleteness == True:
				if downloadComplete( url, f_path ):
					printIfVerbose( "Download of file is complete." )
					return True
				else:
					printIfVerbose( "Download incomplete. Overwriting existing file with fresh download attempt." )
			else:
				return True
		
		# Start the file download as a child process who's progress we can check on to determine the health of the stream
		p = proc(target=dlFile, args=(url, f_path))
		p.start()
		
		file_size = 0 # We initialize the recorded size of the file to 0 bytes.
		num_att = 1 # Intitialize the number of attempts at downloading the file we have made.
		dead_cycles = 0 # Initialize the variable for how many cycles the link has been dead for.
		
		while p.is_alive():
			p.join( proc_check_time )
			
			# During the first iteration of this loop, if the proc_check_time is short enough, execution may reach this line before the subprocess has managed to create the file. To allow for this, we simply wait until the file is created. 
			wait_for_file_creation_count = 0
			while not os.path.exists( f_path ):
				# We can only wait so long until we should assume that some error has occurred in creating the file.
				if wait_for_file_creation_count > file_creation_wait_limit:
					raise FileNotCreatedException( "Target file for %s at %s was not created" % ( url, f_path ) )
				time.sleep( 1 )
				wait_for_file_creation_count += 1
				
			# Once execution reaches this point, we know that the file has been created.
			# We need to check in on p until it returns, and therefore is no longer 'alive'
			if p.is_alive():
				printIfVerbose( "Checking download stream health..." )
				# Get the current size of the file in bytes
				curr_size = os.path.getsize( f_path )
				growth = curr_size - file_size
				printIfVerbose( "Prv size: %s - Curr size: %s - DL Rate: %s B/s" % ( file_size, curr_size, float( growth / proc_check_time ) ) )
				if curr_size > file_size:
					printIfVerbose( "Download stream seems healthy." )
					file_size = curr_size
					dead_cycles = 0
				elif curr_size <= file_size:
					printIfVerbose( "Download stream not looking so healthy." )
					dead_cycles += 1
					if dead_cycles > max_dead_cycles:
						# If the file size has not changed for max_dead_cycles cycles, the stream is likely dead. 
						# We will kill the download and restart it. Hopefully this will solve the problem
						printIfVerbose( "Download stream seems dead. Restart attempt #%s" % num_att )
						num_att += 1 # Keep track of the number of times we've tried to download this file
						# If we have already tried to restart this download the maximum number of times allowed, log the error and move on.
						if num_att > dl_att_thshold:
							p.terminate()
							p.join()
							os.remove( f_path )
							raise DownloadStreamDeadException( "Download stream for %s died and could not be restarted." % url )
						p = restartDL( p, f_path, url )
						file_size = 0

	except Exception as e:
		printIfVerbose(  "Error encountered while downloading %s. Logging event and skipping file." % getNameFromURL( url ) )
		dir, file = os.path.split( f_path )
		name = file.split( '.' )[0]
		err_log_dir = os.path.join( dir, 'error_logs' )
		if not os.path.isdir( err_log_dir ):
			os.mkdir( err_log_dir )
		err_log_fp = os.path.join( err_log_dir, name + '.txt' )
		with open( err_log_fp, 'a' ) as log:
			log.write( "Error while downloading %s from %s\n" % ( name, url ) )
			traceback.print_exc( log )
		return False
		
	return True

# Downloads files from a list of urls passed as a parameter
# list = list of url strings to download
# dl_dir = the direcectory into which the files will be downloaded
def dlFilesFromList( list, dl_dir ):
	for url in list:
		dir, file = os.path.split( url )
		f_path = os.path.join( dl_dir, file )
		executed = dlFileWithProcChecks( url, f_path )
		if executed == True:	
			# Wait a random amount of seconds. I guess some servers will kick you off if you don't wait at all between downloads, and still others will kick you off if you wait exactly the same amount of time between downloads. I've never seen it, but I've read about it and its easy enough to implement.
			factor = random.random() + .5 # Returns a random decimal value between .5 and 1.5
			wait_time = base_wait_time * factor
			printIfVerbose(  "Sleeping for %s seconds..." % wait_time )
			
if __name__ == '__main__':
	main()