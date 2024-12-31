pip install google-cloud-storage google-cloud-bigquery pyspark google-cloud-dataproc

gsutil cp gcs_to_bigquery.py gs://turnkey-cove-443706-t1-dataproc-job/

NOTE: Need to add Storage Admin and BigQuery Admin role to the service account used by the Dataproc Cluster VMs.

NOTE: Make sure to take care of the firewall rules for specified network or subnetwork would likely not permit sufficient communication in the network or subnetwork for Dataproc to function properly. See https://cloud.google.com/dataproc/docs/concepts/network for information on required network setup for Dataproc

gcloud dataproc clusters create dataproc-cluster \
    --region=us-central1 \
    --zone=us-central1-a \
    --image-version=2.2-debian12 \
    --scopes=default,bigquery \
    --subnet=custom-subnet \
    --initialization-actions=gs://goog-dataproc-initialization-actions-us-central1/python/pip-install.sh \
    --metadata='PIP_PACKAGES=google-cloud-bigquery google-cloud-storage' \
    --master-machine-type=e2-standard-2 \
    --worker-machine-type=e2-standard-2 \
    --num-workers=2

NOTE: Last we have to make sure the schemas are matched or else we can get errors which will be hard to resolve (https://github.com/GoogleCloudDataproc/spark-bigquery-connector/blob/master/README.md).

gcloud dataproc jobs submit pyspark gs://turnkey-cove-443706-t1-dataproc-job/gcs_to_bigquery.py \
    --cluster=dataproc-cluster \
    --region=us-central1 \
    -- gs://turnkey-cove-443706-t1-dataproc-job/movie_ratings.csv dataproc_gcs_to_bq dataproc_gcs_to_bq

gcloud dataproc clusters delete dataproc-cluster --region=us-central1
