"""
Wrapper script intended to administrate the download of one particular set of bathymetry data surveys. Over 156,000 surveys are present in the set, and they are stored on a touchy server. This server is set up so that if any single computer tries to open too many ( >3 or so based on my experience) connections to the server, the server closes ALL said connections, and for a period of time afterwards (anywhere from 1 to 12 hours), the server will not allow any connections made from the original computer to stay open to download completion. However, it seems that if multiple computers each make a single connection to the server, no alarms are tripped and no computer is kicked to the curb.

Because our target dataset is so large, downloading all 156,000 files through a single connection will take months. To circomvent this bottleneck, we are attempting to spread the download of all of these files across as many computers as possible. Each participant computer will be given a subset of the files to download, and will download only those files. Afterwards the total dataset will be compiled onto a single drive, and transferred to the federal building in Juneau, where the research team in pursuit of this data will finally have access to it. (Huzzah!)

It is difficult to remotely coordinate the download of these files when the number of computers across which the download will be spread is unknown. To address this difficulty, I have decided to reserve the last 70,000 urls for download at the AOOS facility. These will be split into 70 separate text files, each containing 1000 urls.
This wrapper script will reference a file directory into which any number of said text files can be placed. This script will then compile a list of urls from any and all text files in the directory, and download them The script has a reasonable ability 
to handle poor data input (i.e. input which are not valid download urls, or which are not urls at all) in the sense 
that it will attempt to download the url, catch the inevietable "Cannot download this thing as it is not a url" Error, log it and then move on to the next. That is not very efficient however, so please try not to put anything in the input file directory which is not one or more of the input text files I provide. It's likely that the script can handle it, but it isn't definite.

Author: Tristan Sebens
eMail: tristan.ng.sebens@gmail.com
Phone No: +1-(907)-500-5430

If you have any questions please don't hesitate to contact me, and thank you for your help in this endeavor
"""
import sys
import os
import os.path
import errno
# The MassDownloader script should be in the same directory as this wrapper
cwd = os.getcwd()
sys.path.append( cwd )
import MassDownloader as md
import math
import time
import traceback
from multiprocessing import Process as proc
from multiprocessing import Pool
import re

verbose = True # Set to true if you want the script to decribe its behaviour via the console.

download_directory = os.path.join( cwd, 'downloads') # The location on disk where all downloaded files will be saved to
url_list_directory = os.path.join( cwd, 'urls' ) # The location on disk where all text files containing urls to download will be contained.
completeness_reports_directory = os.path.join( cwd, r'reports\completeness' )

MAX_NUM_PROCS = 25
MAX_OUTPUT_LEN = 80 # Basically the max width, in characters, of the command line prompt


# Make the directories if they don't exist
for dir in ( download_directory, url_list_directory ):
	if not os.path.isdir( dir ):
		os.mkdir( dir )

download_loop_threshold = 10 # The number of times that the main loop will iterate before exiting, regardless of how many urls remain to be downloaded. This is a failsafe designed to catch and kill an infinite loop if there happen to be urls in our url download list which will never sucessfully download. Without this catch, the loop would reattempt to download these files ad infinitum.

# Written by @tzot on Stack Overflow
def mkdir_p( path ):
    try:
        os.makedirs(path)
    except OSError as exc:  # Python >2.5
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise

def printIfVerbose( message ):
	if verbose == True:
		print( message )

# Method to log any errors we encounter
def logError( message, tb ):
	log_time = time.time()
	m, s = divmod( log_time, 60 )
	h, m = divmod( m, 60 )
	timestamp = "%d:%d:%d" % (h, m, s)
	
	datestamp = time.strftime("%d/%m/%Y")
	
	wrapper_error_log_fp = os.path.join( download_directory, 'wrapper_err_log.txt' )
	with open( wrapper_error_log_fp, 'a' ) as err_log:
		err_log.write( "Error encountered at %s on %s\n---------------------------\n" % ( timestamp, datestamp ) )
		err_log.write( message + '\n' )
		err_log.write( tb )
		
# Returns all files present within the passed directory that use the passed extension. Does NOT crawl directory tree.
def findFilesByExtension( dir, ext ):
	files = [os.path.join( dir, f ) for f in os.listdir( dir ) if os.path.isfile( os.path.join( dir, f ) ) and os.path.splitext( f )[1] == ext]
	return files

def getListOfURLSForDownload( dir ):
	printIfVerbose( "Compiling list of urls for download..." )
	# Compile a list of complete file paths for all files in the given directory. Only takes text files.
	files = findFilesByExtension( dir, '.txt' )
	urls = list()
	for fp in files:
		with open( fp ) as f:
			for l in f:
				urls.append( l.rstrip() ) # Strip the newline character off of the end of the url.
	sanitized = sanitizeURLList( urls )
	return sanitized

def sanitizeURLList( urls ):
	pattern = ".*download=.*"
	regex = re.compile( pattern )	
	sanitized = list()
	for url in urls:
		if regex.match( url ):
			pass
		else:
			sanitized.append( url )
	return sanitized
		
		
	
# Function which can take in a list of either filepaths, or urls, and return just the names of the files they refer to, with no file directory path attached.
def getFileNames( list ):
	ret = [os.path.basename( l ) for l in list]
	return ret
	
# Given the download directory where the files will be saved, compile a list of all file names (no file paths) which are present in the directory
def getListOfDownloadedFiles( dir ):
	printIfVerbose( "Compiling list of previously downloaded files")
	# All downloaded files will be of extension .gz
	files = findFilesByExtension( dir, '.gz' )
	# Remove the directory paths
	file_names = getFileNames( files )
	return file_names
	
def findUndownloadedFiles( urls ):
	to_dl = list()
	dl_check_params = list()
	for url in urls:
		name = os.path.basename( url )
		fp = os.path.join( download_directory, name )
		if not os.path.isfile( fp ):
			to_dl.append( url )
		else:
			dl_check_params.append( ( url, fp ) )
	return ( to_dl, dl_check_params )
	
# Returns a line consisting of line, followed by as many iterations of char as are needed to reach a total length of l
def fillLineRemainder( line, char, l ):
	while( len( line ) < l ):
		line += char
	return line
	
# Function which checks each file referenced by the entry in the passed list of file paths, and ensures that the file has been downloaded successfully. If it hasn't, the file is deleted. This ensures that the program will reattempt the download on the next batch of download attempts, because while the url for the deleted file will be present in the url lists, the corresponding file will not be.
# Intended to be run as a daemon process.
# The params should be a list of tuples, the first element of which is a url, and the second element of which is the corresponding file path
def checkFilesForCompleteness( params ):
# If there are no files to check, return immediatly. It is easier to put this check here rather than in the main download loop. This way, we can simply spawn the process, then join it later without checking to see if we ever actually spawned it in the first place. If we spawn the download check process, and there are no files to check, then when we try to join it we'll just join it immediatly.
	if len( params ) <= 0:
		return
	for param in params:
		url = param[0]
		fp = param[1]
		file_name = os.path.basename( fp )
		try:
			if md.downloadComplete( url, fp ) == False:
				os.remove( fp )
				print( fillLineRemainder( "%s is fragmented. Deleting." % file_name, '-', MAX_OUTPUT_LEN-1 ) )
			else:
				print( fillLineRemainder( "%s is intact." % file_name, '+', MAX_OUTPUT_LEN-1 ) )
				f = open( os.path.join( completeness_reports_directory, file_name ), 'w' )
				f.close()
		except IOError:
			continue
		
		
# Accepts a list of parameters for file download completeness checks, and only returns those paramaters who do not have a corresponding file in the completeness_reports_directory. A file present in this directory would indicate that the file has already been checked for completeness and is fully intact.
def getUncheckedFiles( params ):
	not_checked = list()
	for p in params:
		url = p[0]
		fp = p[1]
		report_fp = os.path.join( completeness_reports_directory, os.path.basename( fp ) )
		if not os.path.isfile( report_fp ):
			not_checked.append( p )
	return not_checked
		
# Wrapper for the checkFilesForCompleteness function which spawns a child process to conduct the check while the main process proceeds with the download. Function also returns child process so that the main function can join the child process once the most recent download loop has completed.
def beginCompletenessCheck( params ):	
	mkdir_p( completeness_reports_directory )	
	params = getUncheckedFiles( params )
	print( "%s files have not yet been checked." % len( params ) )
	groups = divideIntoGroups( params, MAX_NUM_PROCS )
	pool = Pool( MAX_NUM_PROCS )
	pool.map( checkFilesForCompleteness, groups )	
	
		
# Divide list into num_groups equal_sized groups, and return groups as a list of groups.
def divideIntoGroups( params, num_groups ):
	param_groups = list()
	param_group_size = int( len( params ) / MAX_NUM_PROCS )
	print( param_group_size )
	for i in range( 0, num_groups ):
		if (i+1)*param_group_size > (len( params ) - 1): # Special case for the last group.
			new_group = params[i*param_group_size:] # Grabs all remaining items
		else:
			print( "%s:%s" % ( i*param_group_size, (i+1)*param_group_size ) )
			new_group = params[i*param_group_size:(i+1)*param_group_size]
		param_groups.append( new_group )
	return param_groups	

def main():
	loop_count = 0
	all_files_downloaded = False # Variable used to ensure that all files are downloaded before the process exits, including lists of urls added after the process began
	while all_files_downloaded == False:
		loop_count += 1
		# Refresh the url list
		urls = getListOfURLSForDownload( url_list_directory )
		printIfVerbose( "Found %s urls for download." % len( urls ) )
		# Refresh the list of downloaded files
		files = getListOfDownloadedFiles( download_directory )
		printIfVerbose( "Found %s file already present on disk." % len( files ) )
		# Using these lists, compile a list of urls which do not have corresponding files in the download directory
		printIfVerbose( "Comparing urls to downloaded files." )
		
		to_dl, dl_check_params = findUndownloadedFiles( urls )
		
		beginCompletenessCheck( dl_check_params )
		# If all files for which we have a download urls have corresponding files in our download directory, we may be done. First we need to check to see if the 
		if len( to_dl ) == 0:
			printIfVerbose( "All files already downloaded." )
			printIfVerbose( "Waiting for download check to finish..." )
			p.join()
			# Once the download check has finished, we recompile the lists to see if anything has changed.
			to_dl, dl_check_params = findUndownloadedFiles( urls )
			# If the number of files to download is still 0, that means that the download check process removed no files, indicating that all downloaded files are complete. 
			if len( to_dl ) == 0:
				all_files_downloaded = True
			# If there are still files to download, then we simply let the download loop run for another cycle. Any files deleted by the download check process well be redownloaded on the next go around.
			continue
			assert False
			
		# If execution reaches this point, then there are in fact urls which need downloading still.
		printIfVerbose( "%s urls do not have corresponding files on disk. Beginning downloads..." % len( to_dl ) )
		if loop_count >= download_loop_threshold:
			# If we have tried to download these urls as many times as the download_loop_threshold allows, what it most likely means is that there is a group of urls which the server simply refuses to let us download. Every time we try, eventually the MassDownloader script gives up, logs the error and moves on. Since they never get their corresponding files added download directory, they're flagged for download every time.
			# We'll make a list of the urls which we were unable to download, and exit the process.
			# There is a chance that this could be erroneously triggered by the late adding of urls to the url directory, at a time when the program has already executed many times. I find this to be a fairly unlikely possiblity however.
			message = "Number of download loop attempts exceeded."
			tb = "No traceback stack"
			logError( message, tb )
			sys.exit()
			
		# If we haven't tripped any of the above conditions, then we must be ready to proceed. Here's where the magic happens...
		md.dlFilesFromList( to_dl, download_directory )
		printIfVerbose( "All queued downloads finished. Checking for additional downloads...")
		
def main_dl_check():
	urls = getListOfURLSForDownload( url_list_directory )
	to_dl, dl_check_params = findUndownloadedFiles( urls )
	print( "%s files to check." % len( dl_check_params ) )
	
	beginCompletenessCheck( dl_check_params )
		
if __name__ == '__main__':
	main()
	print( "Exiting program. \"Thank you for your help!\" -Tristan" )	
			
			
		
		
	
	
	
