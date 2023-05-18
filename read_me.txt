env : final
path : D:\GCP\Final_Tool\Code




--------------------------------------

# Copy the key file to the keys directory
scp /GCP/Poc/GE/project-8-382008-0255604d9896.json romil.t3@35.226.61.2:~/keys/keyfile.json

/home/romil_t3/project-8-382008-0255604d9896.json

===================================

How to run the Automation code on your local machine

1.To begin, clone the entire repository onto your local machine. Open the code.py file in your Notebook and replace the GCP service key under credentials with your own key, which can be obtained from your GCP account.

2.Replace the values for project_id, dataset_name, table_name, bucket_name and the File_name ("file_name = f"GE_8_validation_results_08-05{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", you can update it with the Date.)in the code with the actual names from your GCP account. Be sure to replace all of these values except for config_file.

3.Once you have replaced the service key and other variables with their actual names, run the entire code.

4.In order to upload a specific file to GCP, change the file name in the code to the name of the file you wish to upload (As mentioned in step 2).Once you upload the file, go to the Bucket service and check the file which you have uploaded.

5.It is important to ensure that all of the necessary files are in the same folder or environment in order for the automation tool to function properly.

=====================================================================

How to run the Automation code on the Vitual Machine in GCP.

1. Create one instance by going into the Vitual machine service, name it as test.
2. once you create the instance, click on SSH and open in new browser.
3. This will open the terminal for your VM from where you can run the automation tool.
4. Make sure to upload your Service key (by clicking on upload file) it will get uploaded into your root directory your root directory is romil_t3@test
5. Once you upload the the service key, then you have to configure that path with your credentials, which is availble in your code.
6.So opne the git hub repository, change the credentials path to /home/romil_t3/project-8-382008-0255604d9896.json and save it.
7. Once you save the updated code file, clone the repository in your VM by running git clone (path to repository)
8.install all the necessary libraries by running pip3 install -r requirments.txt and other libraries.
9. change directory and go to the directory where all your reposiroty is clones by using cd command.
10.for ex if you repository name is automation then use cd automation to be in directory and by running ls you will get the list of directories in your directory.
11.run the code python3 code.py

after uploading key, ls and then pwd, it is path

install git sudo apt-et git install, also python 3



1. Create one VM instance on your GCP account.
2. Open the instance by clicking on SSH and open in Browser.
3. Upload your Service key in you root directory by clicking on Upload file tab at top right corner of your terminal.
4. Once upload confirm it by pressing ls command, it will show you your key.
5. Run command pwd to get the desire path for your working directroy, save it (you need to update this path in the code where you have to put the credentials) it will look like '/home/instance name/your service key.json'.
6. Now update the code in git hub, where you have to add all the below details:
project_id, dataset_name, table_name, and bucket_name and Credentials (discssed in step 5), also you have to update the File name which you want to upload in the GCP bucket service which looks like "file_name = f"GE_8_validation_results_08-05{datetime.now().strftime('%Y%m%d_%H%M%S')}.json", you can update it with the Date.
7. Now in your terminal you have install necessary modules like git and pip, so for this use below commands.

Update : sudo apt-get update

For git - sudo apt-get install git

For Python - sudo apt-get install python

Check versions:
git --version
pip3 --version

8. now clone the entire repository by running command git clone "Your repository URL" (Make sure to make the necessary changes in the code mentioned in step 5 and 6) before cloning the repository.
9.Once you clone, for exmaple repository Test, go to the Test direcotry by putting cd Test command in your terminal.
and it will be like /home/username/Test:'ls
10. Check all the necessary files in your home directory which you have cloned from the repository by putting "ls" command.
11. It will give all the files which are required to run the code like main_code.py, requirments, configure.yml, read_me.
12. Now install all the required libraries by running pip install -r requirements.txt (This will install libraries required to run the automation code)
13. Now Run the code, by using below command which is 
python3 main_code.py.
It will run the code and result will be uploaded in the GCP bucket service.
14.You can check by going into the bucket service and find the name of your file.


