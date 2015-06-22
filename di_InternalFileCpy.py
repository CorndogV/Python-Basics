##################################################################################################
#
#  Program Name:  	di_InternalFileCpy.py
#  Author:        	Anthony O'Sullivan
#  Date:          	04/23/2015
#  Version:       	1.0
#  Called from:   	Command Line
#  Location:      	/datastage/app/Inbound/Scripts/Shell 
#
#  Description:   	Copies files based on parameters found in di_InternalFileCpy_${PROJ_NM}.cfg
#                 	and renames them on the remote server with the extension
#                 	.processed.YYYYMMDDHH24MISS. 
#
#  Execution:		di_InternalFileCpy.py
#  Input:			<UniqueID> <ProjectName> <OptionalFlag>
#
#            WHERE:                                                             
#                 	<UniqueID>     = Unique value that will identify a record in the config file.
#                 	<ProjectName>  = DataStage project in which copied file will be landed.
#                 	Optional - <RenameFlag> = If set to "N", script will not rename Inbound file.
#
#  Config File:   	$PROJ_DIR/config/di_InternalFileCpy_${PROJ_NM}.cfg
#                  
#
##################################################################################################
#                      M A I N T E N A N C E   H I S T O R Y
#=================================================================================================
# Revision |                 Description                   |       Name & Date
#=================================================================================================
#   1.0    |                Initial version                |    Anthony O'Sullivan     04/23/2015
#
##################################################################################################

#Import Python built in functions
import sys
import datetime
import os
import glob
import time
import shutil

# Check to make sure the correct number of parameters are passed in.
SCRIPT_NM='di_InternalFileCpy'
SCRIPT_INPUT=raw_input('<UniqueID> <ProjectName> <OptionalFlag>:')
SCRIPT_PARAMS=SCRIPT_INPUT.split()

if len(SCRIPT_PARAMS) > 1 and len(SCRIPT_PARAMS) < 4:
	print 'Correct number of parameters'
else:
	print 'Error: Insufficient arguements.'
	print 'USAGE: <UniqueID> <ProjectName> <RenameFlag> (optional)'
	sys.exit()

# Setup runtime variables and environment
if len(SCRIPT_PARAMS) == 2:
	UNI_ID=SCRIPT_PARAMS[0]
	PROJ_NM=SCRIPT_PARAMS[1]
	RENAME_FLG=None
else:
	UNI_ID=SCRIPT_PARAMS[0]
	PROJ_NM=SCRIPT_PARAMS[1]
	RENAME_FLG=SCRIPT_PARAMS[2]

APP_DIR='/datastage/app'
PROJ_DIR=APP_DIR + '/' + PROJ_NM
CFG_DIR=PROJ_DIR + '/config'
LOG_DIR=PROJ_DIR + '/log'
LOGDT=str(datetime.datetime.now())
LOG_FILE_NM=SCRIPT_NM + '.' + UNI_ID + '.' + PROJ_NM + '.' + LOGDT + '.log'
LOG_FILE_NM=LOG_FILE_NM.replace(':', '-')
LOGFIL=open(os.path.join(LOG_DIR, LOG_FILE_NM), 'w+')
SCRIPTCFG=CFG_DIR + '/' + SCRIPT_NM + '_' + PROJ_NM + '.cfg'
SRC_RENAME='processed.' + LOGDT
SRC_RENAME=SRC_RENAME.replace(':', '-')
LOG_RETAIN_DAYS=7
LOG_RETAIN_SECS=LOG_RETAIN_DAYS*24*60*60
CURTIME=time.time()

# Set wait and check time variables. These control the loop timings and iterations.
wait_time=300
wait_cycles=2
check_time=30
check_cycles=4

# Redirect STDOUT and STDERR to the log.
sys.stdout=LOGFIL
sys.stderr=LOGFIL

# Echo start message to the log.
print LOGDT + ' START: ' + SCRIPT_NM + '.py ' + UNI_ID + ' ' +  PROJ_NM + '.\n'

# Check RENAME_FLG value
if RENAME_FLG is None:
	RENAME_FLG='Y'
elif RENAME_FLG=='N':
	RENAME_FLG='N'
else:
	RENAME_FLG='Y'

# Check for script variables in parameter file SCRIPTCFG.
if os.path.isfile(SCRIPTCFG) == True:
	try:
		CFG_FILE=open(SCRIPTCFG)
	except:
		print LOGDT + ' ERROR: Unable to read script variables from ' + SCRIPTCFG + ' for unique id ' + UNI_ID + ', project ' + PROJ_NM + '.\n'
		sys.exit(1)

	# Make sure config file contains only one line for the unique id.
	count=0
	for line in CFG_FILE:
		line = line.rstrip()
		if not line.startswith(UNI_ID):
			continue
		count=count+1
		linesplit=line.split('|')

	if count > 1:
		print LOGDT + ' ERROR: More than one line found in ' + SCRIPTCFG + ' for unique id ' + UNI_ID + '.\n'
		sys.exit(1)
	elif count < 1:
		print LOGDT + ' ERROR: No lines found in ' + SCRIPTCFG + ' for unique id ' + UNI_ID + '.\n'
		sys.exit(1)

	# Check to make sure line was found and all variables populated
	if len(linesplit)==4:
		src_loc=linesplit[1]
		src_files=linesplit[2]
		target_loc=linesplit[3]
	elif len(linesplit)>4:
		print LOGDT + ' ERROR: Too many values in ' + SCRIPTCFG + ' for unique id ' + UNI_ID + ', project ' + PROJ_NM + '.\n'
		sys.exit(1)
	else:
		print LOGDT + ' ERROR: NULL values in ' + SCRIPTCFG + ' for unique id ' + UNI_ID + ', project ' + PROJ_NM + '.\n'
		sys.exit(1)
else:
	print LOGDT + ' ERROR: Config file ' + SCRIPTCFG + ' does not exist.\n'
	sys.exit(1)

# Test to make sure source and target objects exist.
if os.path.exists(src_loc) == False:
	print LOGDT + ' ERROR: ' + src_loc + ' source directory does not exist.\n'
	sys.exit(1)

if os.path.exists(target_loc) == False:
	print LOGDT + ' ERROR: ' + target_loc + ' target directory does not exist.\n'
	sys.exit(1)

###################
# BEGIN FILE LOOP #
###################

# Initilaize flag for determining if we have received at least one file.
at_least_one="NO"

# If the script made it this far there will be files to retrieve. Loop through each file in FTPLSTFIL.
src_file_list=glob.glob(src_loc + '/' + src_files)

if len(src_file_list)==0:
	print LOGDT + ' WARNING! Cannot find ' + src_files + ' file(s) in directory ' + src_loc + '.\n'
	sys.exit(127)

for src_file_pathname in src_file_list:

	# Get the filename only
	src_file_nm=os.path.basename(src_file_pathname)

	###################
	# BEGIN WAIT LOOP #
	###################
	
	# Set loop for wait time and interval. Initialize counters.
	print LOGDT + ' Initializing wait loop for ' + src_file_nm + '. Wait loop will cycle ' + str(wait_cycles) + ' times and sleep for ' + str(wait_time) + ' seconds between iterations.\n'
	wait_count=1
	# Initialize loop
	while wait_count<=wait_cycles:
		print LOGDT + ' Beginning wait loop iteration ' + str(wait_count) + '.\n'

		#########################
		# BEGIN SIZE CHECK LOOP #
		#########################
	
		# Set loop to check file size and get file if sizes match after sleep time.
		print LOGDT + ' Initializing check loop for ' + src_file_nm + '. Check loop will cycle ' + str(check_cycles) + ' times and sleep for ' + str(check_time) + ' seconds between size checks.\n'
		check_count=1
		# Initialize loop
		while check_count<=check_cycles:
			print LOGDT + ' Beginning check loop iteration ' + str(check_count) + '.\n'
			
			# Make sure file exists before attempting to get sizes.
			if os.path.isfile(src_loc + '/' + src_file_nm):

				# Get file size.
				first_check=os.path.getsize(src_loc + '/' + src_file_nm)
				print LOGDT + ' First file size check returns ' + str(first_check) + ' bytes.\n'

				# Sleep and check again.
				print LOGDT + ' Sleeping for ' + str(check_time) + ' seconds between size checks.\n'
				time.sleep(check_time)
				second_check=os.path.getsize(src_loc + '/' + src_file_nm)
				print LOGDT + ' Second file size check returns ' + str(second_check) + ' bytes.\n'
				
				# Compare the two sizes. If they are the same and not zero bytes get the file.
				if first_check==second_check and first_check<>0 and second_check<>0:
					print LOGDT + ' First and second file check sizes match and are greater than zero for ' + src_file_nm + '. Copying file.\n'
					shutil.copy2(src_loc + '/' + src_file_nm, target_loc)
					if os.path.isfile(target_loc + '/' + src_file_nm):
						file_received="YES"
						# Rename file in the source directory if optional parameter RENAME_FLG has not been set to N.
						print LOGDT + ' Copy successful from ' + src_loc + '/' + src_file_nm + ' to ' + target_loc + '/' + src_file_nm + '.\n'
						if RENAME_FLG<>'N': 
							print LOGDT + ' Renaming file in source directory to ' + src_file_nm + '.' + SRC_RENAME + ' to show that it has been received.\n'
							os.rename(src_loc + '/' + src_file_nm, src_loc + '/' + src_file_nm + '.' + SRC_RENAME)
							if os.path.isfile(src_loc + '/' + src_file_nm + '.' + SRC_RENAME):
								print LOGDT + ' Rename successful from ' + src_loc + '/' + src_file_nm + ' to ' + src_loc + '/' + src_file_nm + '.' + SRC_RENAME + '.\n'
								break
							else:
								print LOGDT + ' WARNING: Unable to rename ' + src_loc + '/' + src_file_nm + ' to ' + src_loc + '/' + src_file_nm + '.' + SRC_RENAME + '.\n'
								break
						else:
							print LOGDT + ' File is not to be renamed on source directory.\n'
							break
					else:
						print LOGDT + ' File copy unsuccessful for ' + src_file_nm + '. Re-check sizes and try again.\n'
				else:
					# Sizes do not match. Wait for the sleep time then try again.
					print LOGDT + ' ' + src_file_nm + ' is still being written. Re-check sizes and try again.\n'
				
			else:
				print LOGDT + ' WARNING: ' + src_file_nm + ' does not exist or is unreadable. Sleep for ' + str(check_time) + ' seconds and try again.\n'
				time.sleep(check_time)
			
			# File has not been copied yet due to copy error or file is still being written. Increment check loop by 1 and try again.
			file_received='NO'
			check_count=check_count+1
		
		#######################
		# END SIZE CHECK LOOP #
		#######################

		# Check to see if file received
		if file_received=='YES':
			at_least_one='YES'
			break
		else:
			wait_count=wait_count+1
			print LOGDT + ' Sleeping for ' + str(wait_time) + ' seconds before checking file sizes again.\n'
			time.sleep(wait_time)

	#################
	# END WAIT LOOP #
	#################

#################
# END FILE LOOP #
#################

# Exit with success status if File Received, otherwise fail.
if at_least_one=='YES':
	# Cleanup logs
	print LOGDT + ' Cleaning up logs older than ' + str(LOG_RETAIN_DAYS) + ' days.\n'
	old_log_files=SCRIPT_NM + '.' + UNI_ID + '.' + PROJ_NM + '.*.log'
	all_log_files=glob.glob(LOG_DIR + '/' + old_log_files)
	for file in all_log_files:
		ftime=os.path.getmtime(file)
		difftime=CURTIME-ftime
		if difftime>LOG_RETAIN_SECS:
			os.remove(file)
			print LOGDT + ' SUCCESS: ' + SCRIPT_NM + '.ksh ' + UNI_ID + ' ' + PROJ_NM + '.\n'
	sys.exit()
else:
	print LOGDT + ' ERROR: ' + SCRIPT_NM + '.ksh ' + UNI_ID + ' ' + PROJ_NM + '. Please see ' + LOGFIL + ' for details.\n'
	sys.exit(127)