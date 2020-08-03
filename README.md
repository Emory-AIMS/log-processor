# Deployment instructions

## Requirements

- Python 3
- Pip 3
- [Boto 3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html)

## Step 1 - Download repository
Download the repository (we will put it on ~/log-processor folder)
```bash
git clone https://gitlab.com/coronavirus-outbreak-control/log-processor ~/log-processor
```

## Step 2 - Config.py
Edit following constants in `/config.py` file:
```python
SQS_QUE_URL_COVAPP = 'XXXXXXXXX'
BUCKET_S3_NAME = 'XXXXXXXXX'
BUCKET_S3_COPY_NAME = 'XXXXXXXXX'
BASE_DIR = 'XXXXXXXXX'
```
Replace the `XXXXXXXXX` with your values.

## Step 3 - Setup service
Get the current user, type `# whoami`

Open `consumer-covapp.service` and edit the `user` value with the name of the current user.

In order to set `consumer-covapp.service` as systemctl service, create a symlink of our service
```bash
sudo ln -s ~/log-processor/consumer-covapp.service /etc/systemd/system/consumer-covapp.service
```
Go in the system directory
```bash
cd /etc/systemd/system/
```
Start the service:
```bash
systemctl start consumer-covapp.service
```
And automatically get it to start at boot:
```bash
systmctl enable consumer-covapp.service
```
To test the system, simply type:
```bash
systemctl status consumer-covapp.service
```
The output should be something like:
```bash
● consumer-covapp.service - Consumer Covid App.
   Loaded: loaded
   Active: active (running)
```
That's it.
