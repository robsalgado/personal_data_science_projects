from googleapiclient import discovery
from google.oauth2 import service_account
import pandas as pd
import json

# Define the credentials for the service account
credentials = service_account.Credentials.from_service_account_file(<PATH TO CREDENTIALS JSON>)

# Define the project id and the job id and format it for the api request
profect_id_name = '<YOUR PROJECT ID>'
project_id = 'projects/{}'.format(profect_id_name)
job_name = '<YOUR JOB NAME>'
job_id = '{}/jobs/{}'.format(project_id, job_name)

# Build the service
ml = discovery.build('ml', 'v1', credentials=credentials)

# Execute the request and pass in the job id
request = ml.projects().jobs().get(name=job_id).execute()

# Get just the best hp values
best_model = request['trainingOutput']['trials'][0]
print('Best Hyperparameters:')
print(json.dumps(best_model, indent=4))

# Or put all the results into a df
# Create a list for each field
trial_id, accuracy, n_comp, alpha,  max_iter, loss,penalty,  = [], [], [], [], [], [], []

# Loop through the json and append the values of each field to the lists
for each in request['trainingOutput']['trials']:
    trial_id.append(each['trialId'])
    accuracy.append(each['finalMetric']['objectiveValue']) 
    n_comp.append(each['hyperparameters']['n_components']) 
    alpha.append(each['hyperparameters']['alpha'])
    max_iter.append(each['hyperparameters']['max_iter'])
    loss.append(each['hyperparameters']['loss'])
    penalty.append(each['hyperparameters']['penalty'])
    
# Put the lsits into a df, transpose and name the columns
df = pd.DataFrame([trial_id, accuracy, n_comp, alpha, max_iter, loss, penalty]).T
df.columns = ['trial_id', 'accuracy', 'n_compnents', 'alpha', 'max_iter', 'loss', 'penalty']

# Display the df
df.head()