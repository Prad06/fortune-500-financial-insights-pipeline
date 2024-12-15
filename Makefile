SHELL:=/bin/bash
-include .env

# Clone repo and move to the repo directory to use the make file
# git clone https://github.com/Prad06/fortune-500-financial-insights-pipeline.git
# sudo apt install make

prerequisites: 
	@sudo apt update && sudo apt -y upgrade
	@sudo apt install docker.io python3.9 python3-pip -y
	@sudo pip install pipenv
	@-sudo groupadd docker
	@-sudo gpasswd -a ${USER} docker
# Do this to make the .env variables available in the session	
	@echo "export $(cat ~/fortune-500-financial-insights-pipeline/.env|xargs)"
# exit
# ssh ${GCP_VM}.${GCP_ZONE}.${GCP_PROJECT_ID}
# @sudo service docker restart # Try not doing this

# Adding - before the command ignores any warning
	@-mkdir ~/bin
	@cd ~/bin; wget https://github.com/docker/compose/releases/download/v2.17.3/docker-compose-linux-x86_64 -O docker-compose
	@chmod +x ~/bin/docker-compose
	@touch ~/.bashrc
	@echo 'export PATH="${HOME}/bin:${PATH}"' >> ~/.bashrc
# echo -e makes the /t /n /r possible escapes

	@source ~/.bashrc
	@make terraform-setup

terraform-setup:
	curl -fsSL https://apt.releases.hashicorp.com/gpg | sudo apt-key add -
	sudo apt-add-repository "deb [arch=amd64] https://apt.releases.hashicorp.com focal main"
	sudo apt-get update && sudo apt-get install terraform

# Variables
GCP_PROJECT_ID := data-engineering-finance
GCP_SERVICE_ACCOUNT_USER := finance-project-sa
GCP_CREDENTIALS_DIR := ~/.google/credentials
GCP_CREDENTIALS_NAME := finance-sa-key.json

# Helper function to enable APIs
enable-api:
	@$(foreach api, iam.googleapis.com iamcredentials.googleapis.com dataproc.googleapis.com, \
		echo "Enabling API: $(api)" && gcloud services list --enabled | grep -q "$(api)" || gcloud services enable "$(api)";)

# Create service account
create-service-account:
	@echo "Creating service account: $(GCP_SERVICE_ACCOUNT_USER)"
	@gcloud iam service-accounts create $(GCP_SERVICE_ACCOUNT_USER) \
		--description="Service account for Finance project" \
		--display-name="$(GCP_SERVICE_ACCOUNT_USER)"

# Add roles to the service account
add-roles:
	@echo "Adding roles to service account: $(GCP_SERVICE_ACCOUNT_USER)"
	@$(foreach role, roles/viewer roles/bigquery.admin roles/storage.admin roles/storage.objectAdmin roles/dataproc.worker roles/dataproc.serviceAgent, \
		echo "Assigning role: $(role)" && \
		gcloud projects add-iam-policy-binding $(GCP_PROJECT_ID) \
		--member="serviceAccount:$(GCP_SERVICE_ACCOUNT_USER)@$(GCP_PROJECT_ID).iam.gserviceaccount.com" \
		--role="$(role)";)

# Download the service account key
download-key:
	@echo "Downloading service account key to $(GCP_CREDENTIALS_DIR)/$(GCP_CREDENTIALS_NAME)"
	@mkdir -p $(GCP_CREDENTIALS_DIR)
	@gcloud iam service-accounts keys create $(GCP_CREDENTIALS_DIR)/$(GCP_CREDENTIALS_NAME) \
		--iam-account=$(GCP_SERVICE_ACCOUNT_USER)@$(GCP_PROJECT_ID).iam.gserviceaccount.com

# Set default application credentials
set-credentials:
	@echo "Setting application default credentials..."
	@export GOOGLE_APPLICATION_CREDENTIALS="$(GCP_CREDENTIALS_DIR)/$(GCP_CREDENTIALS_NAME)"
	@echo "Environment variable GOOGLE_APPLICATION_CREDENTIALS set to $(GCP_CREDENTIALS_DIR)/$(GCP_CREDENTIALS_NAME)"

# Main target to execute all tasks
setup-gcp: enable-api create-service-account add-roles download-key set-credentials
	@echo "GCP setup completed successfully."


terraform-infra: 
	cd ~/fortune-500-financial-insights-pipeline/terraform; terraform init; terraform plan -var="project=${GCP_PROJECT_ID}"; terraform apply -var="project=${GCP_PROJECT_ID}"

airflow-setup:
	cd ~/fortune-500-financial-insights-pipeline/airflow; mkdir -p ./dags ./logs ./plugins; echo -e "AIRFLOW_UID=$$(id -u)" > ./airflow/.env; docker-compose build; docker-compose up -d

airflow-gcloud-init:
# Google credentials variable must be available in the parent session
# Must use $$ for () so that the enclosed function will appear in the shell
	docker exec -it $$(docker ps --filter "name=airflow-worker" --format "{{.ID}}") gcloud init
	docker exec -it $$(docker ps --filter "name=airflow-worker" --format "{{.ID}}") gcloud auth application-default login
# gcloud init
# gcloud auth application-default login
# port forward and go to localhost:8080 in browser, airflow:airflow
clean:
	cd ~/fortune-500-financial-insights-pipeline/airflow; docker-compose down --volumes --rmi all
	cd ~/fortune-500-financial-insights-pipeline/terraform; terraform destroy

vm-down:
	@gcloud compute instances delete ${GCP_VM} --zone ${GCP_ZONE}