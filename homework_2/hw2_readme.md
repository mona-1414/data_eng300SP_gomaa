# Homework 2: BERT on AWS

## Setup

Install dependencies:

```bash
pip install boto3 requests pandas sentence-transformers scikit-learn numpy
```

Set AWS credentials in your notebook before running anything:

```python
s3 = boto3.client("s3", aws_access_key_id=..., aws_secret_access_key=..., aws_session_token=..., region_name="us-east-1")
```

Run all functions in order.

## Expected Outputs in S3 (`gomaa-hw2`)

- `data/ml-1m.zip`
- `embeddings/pre1980_embeddings.npy`
- `embeddings/all_embeddings.npy`
- `recommendations/cold_user.json`
- `recommendations/top_user.json`
- `recommendations/self_user.json`

## AI Usage

Tool: Claude Sonnet 4.6. I used it to debug and generate boilerplate code snippets for some of the pipeline steps. My main prompts focused on debugging uploads to S3, and embeddings generation. I reviewed, tested, and made changes to all the code to ensure it produced the correct outputs.
