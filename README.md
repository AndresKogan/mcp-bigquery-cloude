
# 🚀 BigQuery MPC Connector

This **MPC (Model Context Protocol)** connects your AI to **Google BigQuery**, enabling it to perform all types of data requests — from basic queries to complex operations.

## 🛠️ Setup

To use this connector, you must configure the environment credentials **inside the AI environment** (not locally on your machine).

## 🔐 Configure Google Cloud Credentials

Ensure the AI has access to a [Google Cloud service account key](https://cloud.google.com/iam/docs/creating-managing-service-account-keys) with **BigQuery permissions**.  
Set the following environment variable inside the AI environment:
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials.json"
If you're setting it in Python code:
import os
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/path/to/your/credentials.json"

## 📦 Install Dependencies

Install the BigQuery client library:
pip install google-cloud-bigquery

## ▶️ Run the MPC Server

Launch the server with:
python server.py

## ✅ Notes

- This connector assumes the AI has a runtime that supports Python and environment variables.
- You can perform any valid BigQuery operation through the connected AI interface.

## 📁 Example Project Structure

your-project/
├── server.py
├── credentials.json
├── README.md
└── requirements.txt

## 📬 Questions?

If you need help, feel free to open an issue or contact the maintainer.