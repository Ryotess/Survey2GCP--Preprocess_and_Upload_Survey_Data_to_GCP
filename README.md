# Project-Automation-Survey2GCP
This program cleans and uploads surveys from alchemer/surveymonkey(or any survey in SAV format) to Google Cloud Platform Bigquery database

## File Tree
Please organize your file tree as this structure:<br>
-Survey2GCP<br>
&nbsp;&nbsp;|credentials<br>
&nbsp;&nbsp;|input<br>
&nbsp;&nbsp;|Survey2GCP.py or Survey2GCP.ipynb

## Folder/File Description
- credentials<br>
Put your GCP credentials .json file here<br>
You can follow this article to get the credentials:<br>
https://medium.com/towardsdev/manipulate-google-cloud-platform-gcp-bigquery-with-python-b0c1a04bae28

- input<br>
Please put your Survey Raw Data(.zip) here

- Survey2GCP.ipynb/Survey2GCP.py<br>
Main Program

The following two folder will be generated during to the process of main program：
- output<br>
Unzipped Survey Raw Data in .SAV format

- csv_file<br>
Preprocessed survey .csv files need to be uploaded
