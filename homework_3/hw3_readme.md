# NYC Taxi Analytics - HW3

## Setup

Create and SSH into your EC2 instance:

```bash
ssh -i "your-key.pem" ec2-user@<your-ec2-public-ip>
```

### Install the following dependencies:

```bash
sudo yum install -y java-17-amazon-corretto
pip3 install pyspark pandas pyarrow matplotlib
```

### Download Data

```bash
mkdir -p ~/data/nyctlc
aws s3 cp "s3://de300-hw3-nyctlc-549787090008-us-east-1-an/yellow_tripdata_2026-01.parquet" ~/data/nyctlc/
aws s3 cp "s3://de300-hw3-nyctlc-549787090008-us-east-1-an/green_tripdata_2026-01.parquet" ~/data/nyctlc/
```

### Run (must download locally and copy into your EC2 instance first):

```bash
python3 hw3.py
```

### AI Usage

Tool: Claude Sonnet 4.6. I used it to debug and generate boilerplate code snippets. My main prompts focused on debugging PySpark issues, and displaying and saving outputs to S3. I reviewed, tested, and made changes to all the code to ensure it produced the correct outputs.
