.PHONY: blach infra-up docker-run gen-env docker-run image-push generate-env create-dashboard-key
 

ENV_FILE := transform/.env
username := vtsilidis

INGEST_DIR := ingest
TRANSFORM_DIR := transform
DASHBOARD_DIR := dashboard
INGEST_KEY_NAME := ingestor-gcp-key.json
TRANSFORM_KEY_NAME := transform-gcp-key.json
DASHBOARD_KEY_NAME := dashboard-gcp-key.json
DASHBOARD_TOML_NAME := secrets.toml

INGEST_SECRET_DIR_ABS := $(INGEST_DIR)/.secrets
TRANSFORM_SECRET_DIR_ABS := $(TRANSFORM_DIR)/.secrets
INGESTOR_ENV_FILE := $(INGEST_DIR)/.env
TRANSFORM_ENV_FILE := $(TRANSFORM_DIR)/.env
DASHBOARD_SECRET_DIR_ABS := $(DASHBOARD_DIR)/.streamlit

INGESTOR_KEY_FILE := $(INGEST_SECRET_DIR_ABS)/$(INGEST_KEY_NAME)
TRANSFORM_KEY_FILE := $(TRANSFORM_SECRET_DIR_ABS)/$(TRANSFORM_KEY_NAME)
DASHBOARD_KEY_FILE := $(DASHBOARD_SECRET_DIR_ABS)/$(DASHBOARD_KEY_NAME)
DASHBOARD_TOML_FILE := $(DASHBOARD_SECRET_DIR_ABS)/$(DASHBOARD_TOML_NAME)
INGESTOR_KEY_FILE_REL := ./.secrets/$(INGEST_KEY_NAME)
TRANSFORM_KEY_FILE_REL := ./.secrets/$(TRANSFORM_KEY_NAME)
DASHBOARD_KEY_FILE_REL := ./.secrets/$(DASHBOARD_KEY_NAME)
DASHBOARD_TOML_FILE_REL := ./.secrets/$(DASHBOARD_TOML_NAME)

INFRA_DIR := infrastructure

infra-up:
	gcloud auth login 
	cd ./infrastructure && \
	terraform init && \
	terraform apply -auto-approve

create-ingestor-key:
	@echo "📂 Ensuring secret directory exists..."
	@mkdir -p $(INGEST_SECRET_DIR_ABS)

	@echo "🔍 Fetching configuration from Terraform..."
	$(eval PROJECT_ID := $(shell terraform -chdir=$(INFRA_DIR) output -raw GOOGLE_CLOUD_PROJECT))
	$(eval SA_EMAIL := $(shell terraform -chdir=$(INFRA_DIR) output -raw INGESTOR_SA_EMAIL))
	$(eval BUCKET_NAME := $(shell terraform -chdir=$(INFRA_DIR) output -raw GCS_BUCKET))
	
	@echo "🚀 Creating key for Service Account: $(SA_EMAIL)"
	gcloud iam service-accounts keys create $(INGESTOR_KEY_FILE) \
		--iam-account=$(SA_EMAIL) \
		--project=$(PROJECT_ID)

	@echo "📝 Generating .env file..."
	@echo "INGESTOR_GCP_KEY=$(INGESTOR_KEY_FILE_REL)" > $(INGESTOR_ENV_FILE)
	@echo "GCS_BUCKET_NAME=$(BUCKET_NAME)" >> $(INGESTOR_ENV_FILE)
	@echo "PROJECT_ID=$(PROJECT_ID)" >> $(INGESTOR_ENV_FILE)
	
	@echo "✅ Setup complete!"
	@echo "   Key: $(INGESTOR_KEY_FILE)"
	@echo "   Env: $(INGESTOR_ENV_FILE)"
	@echo "⚠️  REMINDER: Check your .gitignore!"

create-transform-key:
	@echo "📂 Ensuring secret directory exists..."
	@mkdir -p $(TRANSFORM_SECRET_DIR_ABS)

	@echo "🔍 Fetching configuration from Terraform..."
	$(eval PROJECT_ID := $(shell terraform -chdir=$(INFRA_DIR) output -raw GOOGLE_CLOUD_PROJECT))
	$(eval SA_EMAIL := $(shell terraform -chdir=$(INFRA_DIR) output -raw TRANSFORM_SA_EMAIL))
	$(eval BUCKET_NAME := $(shell terraform -chdir=$(INFRA_DIR) output -raw GCS_BUCKET))
	$(eval BQ_DATASET := $(shell terraform -chdir=$(INFRA_DIR) output -raw BQ_DATASET))
	
	@echo "🚀 Creating key for Service Account: $(SA_EMAIL)"
	gcloud iam service-accounts keys create $(TRANSFORM_KEY_FILE) \
		--iam-account=$(SA_EMAIL) \
		--project=$(PROJECT_ID)

	@echo "📝 Generating .env file..."
	@echo "TRANSFORM_GCP_KEY=$(TRANSFORM_KEY_FILE_REL)" > $(TRANSFORM_ENV_FILE)
	@echo "GCS_BUCKET_NAME=$(BUCKET_NAME)" >> $(TRANSFORM_ENV_FILE)
	@echo "PROJECT_ID=$(PROJECT_ID)" >> $(TRANSFORM_ENV_FILE)
	@echo "BQ_DATASET=$(BQ_DATASET)" >> $(TRANSFORM_ENV_FILE)
	
	@echo "✅ Setup complete!"
	@echo "   Key: $(TRANSFORM_KEY_FILE)"
	@echo "   Env: $(TRANSFORM_ENV_FILE)"
	@echo "⚠️  REMINDER: Check your .gitignore!"

create-dashboard-key:
	@echo "📂 Ensuring secret directory exists..."
	@mkdir -p $(DASHBOARD_SECRET_DIR_ABS)

	@echo "🔍 Fetching configuration from Terraform..."
	$(eval PROJECT_ID := $(shell terraform -chdir=$(INFRA_DIR) output -raw GOOGLE_CLOUD_PROJECT))
	$(eval SA_EMAIL := $(shell terraform -chdir=$(INFRA_DIR) output -raw TRANSFORM_SA_EMAIL))
	
	@echo "🚀 Creating key for Service Account: $(SA_EMAIL)"
	gcloud iam service-accounts keys create $(DASHBOARD_KEY_FILE) \
		--iam-account=$(SA_EMAIL) \
		--project=$(PROJECT_ID)

	@python -c "import json; data = json.load(open('$(DASHBOARD_KEY_FILE)')); print('[gcp_service_account]'); [print(f'{k} = {json.dumps(v)}') for k, v in data.items()]" > $(DASHBOARD_TOML_FILE)
	@echo "✅ Successfully created $(SECRETS_FILE)!"

docker-run:
	cd ./transform && \
	docker compose up --build

image-push:
	cd ./transform && \
	docker login && \
	docker build -t $(username)/spark-example5:latest . && \
	docker push $(username)/spark-example5:latest 








generate-env:
	@echo "Extracting Terraform outputs to $(ENV_FILE)..."
	@mkdir -p $(dir $(ENV_FILE))
	@terraform -chdir=infrastructure output -json | jq -r 'to_entries | .[] | "\(.key)=\(.value.value)"' > $(ENV_FILE)
	@echo "Done! $(ENV_FILE) has been updated."

.PHONY: clean-env
clean-env:
	@rm -f $(ENV_FILE)
	@echo "Removed $(ENV_FILE)"



