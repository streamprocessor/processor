# Test locally
npm start

# Deploy Cloud Functions from shell
gcloud functions deploy com_google_analytics_v1_Hit --region europe-west1 --runtime nodejs10 --trigger-http --max-instances 5

# Deploy Cloud Run from shell
gcloud builds submit --config=cloudbuild.yaml . --substitutions=TAG_NAME=0.0.1