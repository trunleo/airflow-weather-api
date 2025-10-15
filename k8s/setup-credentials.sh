#!/bin/bash

# GitHub Credentials Setup Script
# This script helps you create Kubernetes secrets securely

set -euo pipefail

echo "🔐 GitHub Credentials Setup for Kubernetes"
echo "=========================================="

# Check if required tools are available
command -v kubectl >/dev/null 2>&1 || { echo "❌ kubectl is required but not installed. Aborting." >&2; exit 1; }
command -v base64 >/dev/null 2>&1 || { echo "❌ base64 is required but not installed. Aborting." >&2; exit 1; }

# Get GitHub username
read -p "Enter your GitHub username: " GITHUB_USERNAME
if [[ -z "$GITHUB_USERNAME" ]]; then
    echo "❌ GitHub username cannot be empty"
    exit 1
fi

# Get GitHub token (hidden input)
echo "Enter your GitHub Personal Access Token:"
echo "ℹ️  Generate one at: https://github.com/settings/tokens"
echo "ℹ️  Required permissions: repo (for private repos) or public_repo (for public repos)"
read -s GITHUB_TOKEN
echo

read -p "Enter the namespace: " NAMESPACE
if [[ -z "$NAMESPACE" ]]; then
    echo "Namespace empty -> set to default"
    NAMESPACE="default"
fi

if [[ -z "$GITHUB_TOKEN" ]]; then
    echo "❌ GitHub token cannot be empty"
    exit 1
fi

# Validate token format (GitHub PATs start with 'ghp_' or 'github_pat_')
if [[ ! "$GITHUB_TOKEN" =~ ^(ghp_|github_pat_) ]]; then
    echo "⚠️  Warning: Token doesn't match expected GitHub PAT format"
    read -p "Continue anyway? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Encode credentials
USERNAME_B64=$(echo -n "$GITHUB_USERNAME" | base64)
TOKEN_B64=$(echo -n "$GITHUB_TOKEN" | base64)

echo "🔄 Creating Kubernetes secret..."

# Create the secret using kubectl
kubectl create secret generic git-credentials \
    --from-literal=GIT_SYNC_USERNAME="$USERNAME_B64" \
    --from-literal=GIT_SYNC_PASSWORD="$TOKEN_B64" \
    --from-literal=GITSYNC_USERNAME="$USERNAME_B64" \
    --from-literal=GITSYNC_PASSWORD="$TOKEN_B64" \
    --namespace "$NAMESPACE" \
    -o yaml > secret.yaml

echo "✅ Secret created successfully!"
echo "📁 Secret saved to: secret.yaml"
echo ""
echo "⚠️  SECURITY REMINDERS:"
echo "   • The secret file contains base64-encoded credentials (NOT encrypted)"
echo "   • Add secret.yaml to .gitignore to avoid committing credentials"
echo "   • Apply the secret to your cluster: kubectl apply -f secret.yaml"
echo "   • Consider using external secret management in production"
echo ""
echo "🔍 To verify the secret:"
echo "   kubectl get secret git-credentials -o yaml"