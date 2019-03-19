#!/usr/bin/perl
## Virag Sharma, March 2018

use strict;
use warnings;
use Getopt::Long qw(:config no_ignore_case no_auto_abbrev);

my $action    = $ARGV[0];
my $jobs_name = $ARGV[1];
my $file_in   = $ARGV[2];

## Get other optional parameters. Needed when pushing jobs
my $n_cores = 0;
my $memory = my $wall_time = "";
GetOptions ("cores:i"  => \$n_cores, "memory:s" => \$memory, "time:s" =>\$wall_time);

my $usage = "USAGE: $0 action[push|crashed|stop|clean] jobs_name jobs_file  
OPTIONAL PARAMETERS: -cores [number_of_cores] -memory [max memory in GB, for example 3 --> this would imply 3GB of memory] -time [max time allowed in hours]\n\n";
my $purpose = "PURPOSE: $0 reads in a jobs file (where every line is a job) and submits these jobs to a SGE cluster. 
$0 is basically a wrapper to submit a list of jobs and to monitor these jobs. Could also be used to stop running jobs,
or to get a list of jobs that crashed from this given list.";

die $usage.$purpose if (scalar(@ARGV) < 2);  ## need at least two parameters -- the action and the job name.

#### A couple of sanity checks to ensure that all parameters are ok:
$action = lc($action);
die "Undefined action '$action'\n" if ($action ne "push" && $action ne "stop" && $action ne "crashed" && $action ne "clean");
die "Insufficient number of arguments for the action 'push'. You need atleast 3\n" if ($action eq "push" && scalar(@ARGV) < 3);

if ( ($action eq "stop" || $action eq "crashed" || $action eq "clean") && (scalar(@ARGV) > 2) ) {
	print "WARNING !! You only need 2 parameters with the action '$action'. Barring the first two parameters, everything else will be ignored\n";	
}

my $home_dir = `pwd`; chomp $home_dir;
my $user = `echo \$USER`; chomp $user;

if ($action eq "clean") {
	action_clean();	
}

if ($action eq "stop") {
	action_stop();	
}

if ($action eq "crashed") {
	action_crashed();	
}

## otherwise push jobs
if (-d "$home_dir/\.jobs_input/$jobs_name") {
	die "ERROR!! The jobs name directory exists. Delete this and resubmit i.e. do a 'rm -r $home_dir/\.jobs_input/$jobs_name' and resubmit\n";
} else {
	## create relevant directories
	`mkdir -p $home_dir/\.jobs_input/$jobs_name`;
	`mkdir -p $home_dir/\.jobs_input/$jobs_name/stdout`;
	`mkdir -p $home_dir/\.jobs_input/$jobs_name/stderr`;
}

my $stdout_dir = "$home_dir/\.jobs_input/$jobs_name/stdout";
my $stderr_dir = "$home_dir/\.jobs_input/$jobs_name/stderr";

my %jobs_id_hash = (); ## a hash where the key is the job identifier and the value is the job (i.e. the particular line from the jobs file)
my $ct = 0;

### create a job_info file that contains all the job ids
open(FOS,">$home_dir/\.jobs_input/$jobs_name/jobIDs.txt") || die "Error writing to the jobIDs file\n";
open(FI,$file_in) || die "Error opening input file '$file_in'\n";
while (my $line = <FI>) {
	$line =~s/\s+$//;
	
	my $job_script  = "$home_dir/\.jobs_input/$jobs_name/o.$ct";
	my $stdout      = "$stdout_dir/o.$ct";
	my $stderr      = "$stderr_dir/o.$ct";
	create_job_file($stdout,$stderr,$job_script,$line);
	
	## submit the job
	my $job_submission_call = "qsub -V $job_script";
	my $job_id_line = `$job_submission_call`;
	die "ERROR: $job_submission_call failed\n" if ($? != 0 || ${^CHILD_ERROR_NATIVE} != 0);
	
	my $job_id = (split / /,$job_id_line)[2];
	$jobs_id_hash{$job_id} = $ct;
	print FOS "$job_id\n";
	$ct++;
}
close FI;
close FOS;
sleep 10;

my @all_job_ids = keys(%jobs_id_hash);
my $total_jobs = $ct;

## All jobs have been submitted, now check their status
my $jobs_all_done = my $jobs_failed = 0;
my $time_total = 0;

while ($jobs_all_done != 1) {	
	## get status of all jobs
	my $failed = my $finished = my $running = my $pending = 0; ## Now see how many are running and how many are pending

	foreach my $job_id(@all_job_ids) {
		my $stat = `qstat -u $user|grep -w $job_id`;
		
		if ($stat eq "") {
			## check if the job crashed
			my $ct = $jobs_id_hash{$job_id};
			my $std_out = "$stdout_dir/o.$ct";
			
			my $failed_job = check_job_failed($std_out);
			if ($failed_job == 1) {
				$failed++;	
			} else {
				$finished++;	
			}
			next;
		}
		
		$stat =~s/^\s+//;
		my ($job_id,$status) = (split /\s+/,$stat)[0,4];
		if ($status eq "r") {
			$running++;
		} else {	
			$pending++;	
		}
	}

	if ($failed + $finished == $total_jobs) {  ## eventually a job will either finish or crash
		$jobs_all_done = 1;
	} else {
		print "###############\n\n\n";
		print "Total number of submitted jobs for '$jobs_name' is '$total_jobs'\nJobs finished ==> '$finished', jobs running ==> '$running', jobs pending ==> '$pending', jobs failed ==> '$failed'\n";
		print "###############\n\n\n";
		print "Waited for $time_total seconds, so far\n";
		sleep 60;
		$time_total += 60;	
	}
	$jobs_failed = $failed;
}

if ($jobs_failed != 0) {
	die "The operation did not go through, '$jobs_failed' jobs failed. Please check  $home_dir/\.jobs_input/$jobs_name\n";
} else {
	print "SUCCESS!! All the jobs finished successfully\n";
}

######## Functions #####

sub action_clean {
	my $jobs_dir = $home_dir."/.jobs_input/$jobs_name";
	die "The jobs directory '$jobs_dir' does not exist. So nothing will be cleaned\n" if (! -d $jobs_dir);
	
	my $delete_call = "rm -r $jobs_dir";
	system($delete_call) == 0 || die "Error cleaning the jobs directory '$jobs_dir'\n";
	exit;
}

sub action_crashed {
	## find the stdout directory associated with the jobs list
	my $jobs_stderr = "$home_dir/\.jobs_input/$jobs_name/stdout";
	my $ls = `ls $jobs_stderr`;
	
	foreach my $file(split /\n/,$ls) {
		my $stdout_file = "$jobs_stderr/$file";
		my $job_ct = $file;
		$job_ct =~s/o.//;
		
		my $job_status = check_job_failed($stdout_file);
		if ($job_status == 1) {
			my $job_file = "$home_dir/\.jobs_input/$jobs_name/o.$job_ct";
			my $job_line = extract_job_line($job_file);
			print "$job_line\n";	
		}
	}
	exit;
}

sub action_stop {
	## Get the job-ids file that contains information about the current set of jobs and then do a qdel on these jobs
	my $job_ids_file = "$home_dir/\.jobs_input/$jobs_name/jobIDs.txt";
	die "Cannot find the job_id file '$job_ids_file'. Aborting\n" if (! -e $job_ids_file);
	open(FI,"$job_ids_file");
	while (my $job = <FI>) {
		chomp $job;
		`qdel $job`;
	}
	close FI;
	print "Stopped jobs for the jobsList '$jobs_name'\n";
	exit;
}

sub create_job_file {
	my ($stdout,$stderr,$job_file,$line) = @_;
	open(FO,">$job_file") || die "Error writing to jobs input file '$job_file'\n";
	print FO "#!/bin/bash\n";
	print FO "#\$ -N $jobs_name\n";
	print FO "#\$ -j n\n";
	print FO "#\$ -pe smp $n_cores\n" if ($n_cores != 0);
	print FO "#\$ -V\n";
	print FO "#\$ -l mem_free=$memory"."G\n" if ($memory ne "");
	print FO "#\$ -l h_rt=$wall_time:00:00\n" if ($wall_time ne "");
   	print FO "#\$ -m beas\n\n";
	print FO "#\$ -o $stdout\n";
	print FO "#\$ -e $stderr\n";
	print FO "$line || echo \"Job crashed\"\n";
	close FO;
}

sub check_job_failed {
	my $std_out_file = shift;
	open(FIE,"$std_out_file") || die "Error opening the file '$std_out_file' in check_job_failed function\n";
	my @file_list = <FIE>;
	close FIE;
	
	my $last_line = $file_list[$#file_list];
	$last_line =~s/\s+$//;
	my $job_crashed = 0;
	$job_crashed = 1 if ($last_line eq "Job crashed");
	return $job_crashed;
}

sub extract_job_line {
	my $job_file = shift;
	my $job_line = `grep -v "^#" $job_file`;
	$job_line =~s/ \|\| echo "Job crashed"//;
	chomp $job_line;
	$job_line =~s/^\s+//;
	return $job_line;
}
