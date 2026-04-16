#!/bin/bash

# DP-750 Lab Setup Script
# Creates an Azure Databricks Premium workspace in a randomly selected region.

set -e

# Select a random Azure region that supports Azure Databricks
REGIONS=( australiaeast australiasoutheast brazilsouth canadacentral canadaeast centralindia centralus eastasia eastus eastus2 francecentral germanywestcentral japaneast japanwest koreacentral northcentralus northeurope norwayeast southcentralus southeastasia southindia swedencentral switzerlandnorth uksouth ukwest westeurope westus westus2 westus3 )
REGION=${REGIONS[$RANDOM % ${#REGIONS[@]}]}

RESOURCE_GROUP="rg-dp750"
WORKSPACE_NAME="adb-dp750"

echo "Installing az databricks extension..."
az config set extension.dynamic_install_allow_preview=true
az extension add --upgrade -n databricks

echo "Deploying to region: $REGION"

# Create the resource group
az group create \
  --name $RESOURCE_GROUP \
  --location $REGION

# Create the Azure Databricks Premium workspace
az databricks workspace create \
  --resource-group $RESOURCE_GROUP \
  --name $WORKSPACE_NAME \
  --location $REGION \
  --sku premium

echo "Installation done"
