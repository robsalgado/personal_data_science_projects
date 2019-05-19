import argparse
from google.cloud import storage
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.linear_model import SGDClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
import pandas as pd
from sklearn.externals import joblib
import hypertune

# Create the argument parser for each parameter plus the job directory
parser = argparse.ArgumentParser()

parser.add_argument(
    '--job-dir',  # Handled automatically by AI Platform
    help='GCS location to write checkpoints and export models',
    required=True
    )
parser.add_argument(
    '--n_components',  # Specified in the config file
    help='k best features per class',
    default=100,
    type=int
    )
parser.add_argument(
    '--alpha',  # Specified in the config file
    help='Constant that multiplies the regularization term',
    default=0.0001,
    type=float
    )
parser.add_argument(
    '--max_iter',  # Specified in the config file
    help='Max number of iterations.',
    default=1000,
    type=int
    )
parser.add_argument(
    '--loss',  # Specified in the config file
    help='Loss function to be used',
    default='hinge',
    type=str
    )
parser.add_argument(
    '--penalty',  # Specified in the config file
    help='The penalty (aka regularization term) to be used',
    default='l2',
    type=str
    )

args = parser.parse_args()

# Define the GCS bucket the training data is in
bucket = storage.Client().bucket('training_jobs_bucket')

# Define the source blob name (aka file name) for the training data
blob = bucket.blob('train.csv')

# Download the data into a file name
blob.download_to_filename('train.csv')

# Open the csv into a df
with open('./train.csv', 'r') as df_train:
    df = pd.read_csv(df_train)

# Put the clean text into a variable for processing
texts = df['clean_text'].astype('str')

# Create the features
tfidf_vectorizer = TfidfVectorizer(
    ngram_range=(1, 2),
    min_df=2,
    max_df=.95
    )

# Defining features (X) and target (y)
X = tfidf_vectorizer.fit_transform(texts)
y = df['label_num'].values

# Dimenionality reduction
lsa = TruncatedSVD(
    n_components=args.n_components,  # Will tune this parameter as well
    n_iter=10
    )

X = lsa.fit_transform(X)

# Train test split
X_train, X_test, y_train, y_test = train_test_split(
    X,
    y,
    test_size=.25,
    shuffle=True
    )

# Define the model with the parameters we want to tune
model = SGDClassifier(
    alpha=args.alpha,
    max_iter=args.max_iter,
    loss=args.loss,
    penalty=args.penalty
    )

# Fit the training data and predict the test data
model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# Define the score we want to use to evaluate the classifier on
score = accuracy_score(y_test, y_pred)

# Calling the hypertune library and setting our metric
hpt = hypertune.HyperTune()
hpt.report_hyperparameter_tuning_metric(
    hyperparameter_metric_tag='accuracy',
    metric_value=score,
    global_step=1000
    )

# Export the model to a file. The name needs to be 'model.joblib'
model_filename = 'model.joblib'
joblib.dump(model, model_filename)

# Define the job dir, bucket id and bucket path to upload the model to GCS
job_dir = args.job_dir.replace('gs://', '')  # Remove the 'gs://'

# Get the bucket Id
bucket_id = job_dir.split('/')[0]

# Get the path
bucket_path = job_dir.lstrip('{}/'.format(bucket_id))

# Upload the model to GCS
bucket = storage.Client().bucket(bucket_id)
blob = bucket.blob('{}/{}'.format(
    bucket_path,
    model_filename
    )
)
blob.upload_from_filename(model_filename)
