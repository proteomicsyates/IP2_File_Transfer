# IP2_File_Transfer
Giving some SSH credentials to a file server with the files of IP2, it will explore a given list of IP2 projects to get all RAW files and DTASelect files and transfer them to an external FTP site, such as MassIVE

## How to run it:
First, you need to write the input file paths created. The better way to do it is to run class:  
`edu.scripps.yates.ip2tomassive.InputFileGenerator`  


with a single parameter with the input parameters full path.  
  
This program whould generate a file that will be used in the second part. This file is 
```
DATASET dataset_name

raw-files
raw_data_full_path/raw_data_file1.raw
raw_data_full_path/raw_data_file2.raw
raw_data_full_path/raw_data_file3.raw

DTASelect-files
dtaselect_data_full_path/DTASelect-file1.raw
dtaselect_data_full_path/DTASelect-file2.raw
dtaselect_data_full_path/DTASelect-file3.raw
```

Then, you need to run a second script from class:  
`edu.scripps.yates.ip2tomassive.MultiProjectUpload`  

with two parameters:
 * the full path to the properties file (the same as the one used in the previous script).  
 * the full path to the output file generated from previous script.
 

## The input properties file format:
You can download a template from here: [ip2FileTransfer.properties](https://github.com/proteomicsyates/IP2_File_Transfer/raw/master/ip2FileTransfer.properties)
```
# ip2 server (SSH if port 22) login info
ip2_server_url = ip2_server_host_name
ip2_server_user_name = your_user_name
ip2_server_password = *****
ip2_server_connection_port = 22
ip2_server_project_base_path = full_path_to_project_base_folder


# massive FTP login info
massive_server_url = massive.ucsd.edu
massive_server_user_name = your_user_name
massive_server_password = ******

 
# information about the project in IP2
project_name = name of your project    # this will be used to create a folder with that name in your FTP site in the remote host
experiment_ids = 13602,13622,13630,13651,13694,14439,14440,13748  # number ids of the experiments you want to transfer

```
