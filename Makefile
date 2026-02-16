.PHONY: blach infra-up docker-run gen-env docker-run image-push generate-env
 

ENV_FILE := transform/.env
username := vtsilidis

INGEST_DIR := ingest
INGEST_KEY_NAME := ingestor-gcp-key.json

INGEST_SECRET_DIR_ABS := $(INGEST_DIR)/.secrets
INGESTOR_ENV_FILE := $(INGEST_DIR)/.env


INGESTOR_KEY_FILE := $(INGEST_SECRET_DIR_ABS)/$(INGEST_KEY_NAME)
INGESTOR_KEY_FILE_REL := ./.secrets/$(INGEST_KEY_NAME)

INFRA_DIR := infrastructure

.PHONY: create-key

generate-env:
	@echo "Extracting Terraform outputs to $(ENV_FILE)..."
	@mkdir -p $(dir $(ENV_FILE))
	@terraform -chdir=infrastructure output -json | jq -r 'to_entries | .[] | "\(.key)=\(.value.value)"' > $(ENV_FILE)
	@echo "Done! $(ENV_FILE) has been updated."

.PHONY: clean-env
clean-env:
	@rm -f $(ENV_FILE)
	@echo "Removed $(ENV_FILE)"

docker-run: generate-env
	cd ./transform && \
	docker compose up --build



image-push:
	cd ./transform && \
	docker login && \
	docker build -t $(username)/spark-example4:latest . && \
	docker push $(username)/spark-example4:latest

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