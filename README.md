# Test_Automation
Document This document provides a detailed guide on executing the automation code Google Cloud Platform (GCP) instance. Please follow the steps below to ensure a successful execution.

Running Automation Code on GCP Instance

Step 1: Create a VM Instance Within your GCP account, create a new VM instance to serve as the execution environment for the automation code.

Step 2: Access the GCP Instance Open the GCP instance by clicking on the "SSH" button and selecting "Open in Browser". This will establish a secure shell connection to the instance.

Step 3: Upload the Service Key Within the GCP instance terminal, click on the "Upload file" option located at the top-right corner. Choose the service key file from your local system and upload it to the root directory of the instance.

Step 4: Confirm Service Key Upload To confirm the successful upload, execute the "ls" command in the terminal. The service key file should be listed among the displayed files.

Step 5: Retrieve Service Key Path Obtain the path of the current working directory where the service key is uploaded by running the pwd command. Save this path, as it will be required for the code configuration later on. The path will typically follow this format: '/home/instance_name/your_service_key.json'.

Step 6: Update Code Configuration** Access the automation code in the GitHub repository and update the following details within the code: Project ID: Specify the ID of your GCP project.

Dataset Name: Provide the name of the dataset to be used.

Table Name: Specify the name of the target table.

Bucket Name: Specify the name of the GCP bucket for file uploads.

Credentials Path: Update the code with the path obtained in Step 5 to ensure correct authentication.

Step 7: Install Required Modules In the GCP instance terminal, execute the following commands to install the necessary modules: sudo apt-get update

sudo apt-get install git

sudo apt-get install python

Verify successful installation by checking the versions using

git --version and pip3 --version.

Step 8: Clone the Repository Clone the entire repository containing the automation code using the command git clone <repository_URL>. Before cloning, ensure that the required changes from Step 6 have been applied to the code.

Step 9: Navigate to the Repository Directory Change the directory to the cloned repository by executing the command cd <directory_name>.

Step 10: Verify Required Files Confirm the presence of all necessary files for executing the code by running the ls command. Ensure that the files,
data_validator.py 
Working_Code.py, 
requirements.txt, 
Data_Configuration.yml,
read_me are listed.

Step 11: Install Required Libraries Install the required libraries by running the command pip install -r requirements.txt. This will install all the necessary dependencies for the automation code.

Step 12: Execute the Code Run the automation code by executing the command python3 Working_Code.py in the terminal. The code will initiate and the resulting output will be uploaded to the designated GCP bucket service.

Step 13: Verify Upload Confirm the successful upload by accessing the GCP bucket service and locating the file with the specified name. Following these instructions meticulously will ensure the proper execution of the automation code on your GCP instance. If you encounter any issues or require further assistance, please do not hesitate to seek support.
