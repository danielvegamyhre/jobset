gcloud compute networks create scaletestnetwork \
  --project=${project} \
  --subnet-mode=custom

gcloud beta container clusters create \
  --addons=NodeLocalDNS \
  --cluster-ipv4-cidr=/10 \
  --create-subnetwork=range=/18 \
  --enable-ip-alias \
  --enable-private-nodes \
  --logging=SYSTEM,WORKLOAD,API_SERVER,SCHEDULER,CONTROLLER_MANAGER \
  --master-ipv4-cidr=172.16.0.0/28 \
  --monitoring=SYSTEM,API_SERVER,SCHEDULER,CONTROLLER_MANAGER \
  --no-enable-master-authorized-networks \
  --quiet \
  --services-ipv4-cidr=/16 \
  --project=${project} \
  --region=us-central1 \
  --network=scaletestnetwork \
  --num-nodes=666 \
  --machine-type=e2-medium \
  --disk-size=30GB \
  --node-locations=us-central1-a,us-central1-c,us-central1-f \
  --cluster-version=1.29.2-gke.1521000 \
  scaletestcluster

gcloud compute firewall-rules create \
  scaletestcluster-us-central1 \
  --project=${project} \
  --network=scaletestnetwork \
  --allow=tcp:22,tcp:80,tcp:8080,tcp:9090,tcp:30000-32767,udp:30000-32767,tcp:9091 \
  --target-tags=gke-scaletestcluster-ed4e8f0a-node

gcloud compute routers create \
  scaletestrouter \
  --project=${project} \
  --network=scaletestnetwork \
  --region=us-central1

gcloud compute routers nats create \
  scaletestnat \
  --project=${project} \
  --router=scaletestrouter \
  --router-region=us-central1 \
  --auto-allocate-nat-external-ips \
  --min-ports-per-vm=64 \
  --nat-primary-subnet-ip-ranges

gcloud beta container node-pools create \
  heapster-pool \
  --cluster=scaletestcluster \
  --project=${project} \
  --region=us-central1 \
  --num-nodes=1 \
  --machine-type=n1-standard-64

gcloud beta container node-pools create \
  pool1 \
  --cluster=scaletestcluster \
  --project=${project} \
  --region=us-central1 \
  --num-nodes=666 \
  --disk-size=30GB \
  --machine-type=e2-medium

gcloud beta container node-pools create \
  pool2 \
  --cluster=scaletestcluster \
  --project=${project} \
  --region=us-central1 \
  --num-nodes=666 \
  --disk-size=30GB \
  --machine-type=e2-medium