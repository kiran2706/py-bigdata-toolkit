# Local Git Credentials Setup Guide

This guide explains how to set up Git credentials locally for this specific repository.

## Prerequisites

- Git installed on your system
- A GitHub Personal Access Token (PAT)
- Access to the repository

## Setup Steps

### 1. Clear Global Credential Settings

Ensure no global credential settings interfere with your local setup:

```bash
git config --global --unset credential.helper
```

### 2: Enable Local Credential Storage

Set up Git to save your credentials specifically for this repository:

```bash
git config credential.helper store
```

### Step 3: Add Remote URL with Personal Access Token (PAT)

Update the repository's remote URL to include your GitHub username and your PAT:

```bash
git remote set-url origin https://<USERNAME>:<TOKEN>@github.com/<USERNAME>/<REPO>.git
```
Replace the placeholders with your actual values:

<USERNAME> – Your GitHub username

<TOKEN> – Your GitHub Personal Access Token (PAT)

<REPO> – The name of your GitHub repository

### Step 4: Push to Save Credentials Locally

Once you've configured the remote URL, push your branch to the repository:

```bash
git push origin <your-branch-name>
```