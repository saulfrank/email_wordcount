# Email word count analysis script


## deploy and configure server

```
On AWS deploy EC2
Instance type: t2.medium
Region: US-EAST-1a (for https://aws.amazon.com/datasets/enron-email-data/)
Linux: Ubuntu
Boot drive: 200GB
Attach snapshot to 400GB drive: snap-d203feb5 

Download pem key to ssh in and:
sudo chmod 600 saul.pem (on local computer to allow for connection)
ssh -p 22  -i saul.pem ubuntu@ ec2-xx-xxx-xxx-xxx.compute-1.amazonaws.com

Upgrade to Ubuntu 16
sudo do-release-upgrade

Check python version
python –V
Python 2.7.12 (version we used)

Mount snapshot drive:
sudo mkdir /mnt/email
sudo mount /dev/xvdb /mnt/email

create directory and upload GIT files:
mkdir /home/ubuntu/email_report/
cd /home/ubuntu/email_report/

```



## install

```
in /home/ubuntu/email_report/ directory:
python -m venv
pip install -r requirements.txt
```

## Run on remote server

```

cd /home/ubuntu/email_report
nohup ./venv/bin/python app.py &

```

## Assumptions

```
Counts words in the text/plain part of a multi-part email
Top emails count includes any internal emails

```
