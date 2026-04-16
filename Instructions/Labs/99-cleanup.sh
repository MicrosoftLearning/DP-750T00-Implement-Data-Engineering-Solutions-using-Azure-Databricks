#!/bin/bash

# DP-750 Lab Cleanup Script
# Deletes the resource group and all resources within it.

set -e

RESOURCE_GROUP="rg-dp750"

echo "Deleting resource group: $RESOURCE_GROUP and all resources within it..."

az group delete \
  --name $RESOURCE_GROUP \
  --yes \
  --no-wait

echo "Resource group deletion initiated. It may take a few minutes to complete."
