#!/bin/bash

# DP-750 Lab Setup Script
# Creates an Azure Databricks Premium workspace in a randomly selected region.
#
# Usage: ./00-setup.sh [--initial-catalog-name <name>]
#   --initial-catalog-name  Optional. Sets the initial name of the default Unity Catalog.

set -e

# Parse optional arguments
INITIAL_CATALOG_NAME=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --initial-catalog-name)
      INITIAL_CATALOG_NAME="$2"
      shift 2
      ;;
    *)
      echo "Unknown argument: $1"
      exit 1
      ;;
  esac
done

# Select a random Azure region that supports Azure Databricks
REGIONS=( australiaeast australiasoutheast brazilsouth canadacentral canadaeast centralindia centralus eastasia eastus eastus2 francecentral germanywestcentral japaneast japanwest koreacentral northcentralus northeurope norwayeast southcentralus southeastasia southindia swedencentral switzerlandnorth uksouth ukwest westeurope westus westus2 westus3 )
REGION=${REGIONS[$RANDOM % ${#REGIONS[@]}]}

RESOURCE_GROUP="rg-dp750"
WORKSPACE_NAME="adb-dp750"

echo "Installing az databricks extension..."
az config set core.collect_telemetry=no 2>/dev/null
az config set core.display_warnings=no 2>/dev/null
az config set extension.dynamic_install_allow_preview=true 2>/dev/null
az extension add --upgrade -n databricks

echo "Creating resource group $RESOURCE_GROUP in region $REGION..."
az group create \
  --name $RESOURCE_GROUP \
  --location $REGION

echo "Creating Azure Databricks Premium workspace $WORKSPACE_NAME..."

# Build optional default-catalog argument
DEFAULT_CATALOG_ARG=""
if [[ -n "$INITIAL_CATALOG_NAME" ]]; then
  DEFAULT_CATALOG_ARG="--default-catalog initial-name=$INITIAL_CATALOG_NAME initial-type=UnityCatalog"
fi

az databricks workspace create \
  --resource-group $RESOURCE_GROUP \
  --name $WORKSPACE_NAME \
  --location $REGION \
  --sku premium \
  $DEFAULT_CATALOG_ARG

echo "Installation done"
