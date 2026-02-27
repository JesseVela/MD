# AWS credentials for load_vec_to_rds.py (Bedrock)

The script calls **Amazon Bedrock** to generate embeddings. If you see:

```text
Done. Inserted/updated 0 rows ...
Errors (1205):
G00066: Unable to locate credentials
...
```

then **boto3 cannot find AWS credentials**. The script never gets valid embeddings, so nothing is written to the database.

---

## Fix: give boto3 credentials

Use **one** of the methods below. After that, run the script again.

### 1. AWS CLI configured (simplest)

If you have an **IAM user** with Access Key ID and Secret Access Key, use `aws configure` (see step-by-step below).  
If you only sign in to the AWS Console via **SSO or a role** (e.g. **gch-agentic-poc-role**) and don’t have access keys, use **Section 3 (AWS SSO)** instead.

#### Step-by-step: `aws configure`

1. **Open PowerShell** and run:
   ```powershell
   aws configure
   ```

2. **AWS Access Key ID**  
   - Prompt: `AWS Access Key ID [None]:`  
   - **What to enter:** Your access key (e.g. starts with `AKIA...`).  
   - **Where to get it:** In the AWS Console go to **IAM → Users → your user → Security credentials → Access keys → Create access key**. Copy the Access Key ID.  
   - If you don’t have an IAM user or can’t create keys, use **Section 3 (AWS SSO)** instead.

3. **AWS Secret Access Key**  
   - Prompt: `AWS Secret Access Key [None]:`  
   - **What to enter:** The secret key shown only once when you created the access key (e.g. a long string of letters/numbers/symbols).  
   - **Tip:** Paste it; it won’t show on screen.

4. **Default region name**  
   - Prompt: `Default region name [None]:`  
   - **What to enter:**  
     **`us-east-1`**  
   - (Matches “United States (N. Virginia)” / your current region in the Console.)

5. **Default output format**  
   - Prompt: `Default output format [None]:`  
   - **What to enter:**  
     **`json`**  
   - (Or press Enter to leave default; `json` is fine for the script.)

6. **Check it worked:**
   ```powershell
   aws sts get-caller-identity
   ```
   You should see your account ID and user/role. Then run:
   ```powershell
   python db/load_vec_to_rds.py
   ```

Credentials are stored under `%USERPROFILE%\.aws\credentials`. You don’t need to set env vars; boto3 will use this config.

---

### 2. Environment variables (CI or one-off)

Set these in PowerShell **before** running the script:

```powershell
$env:AWS_ACCESS_KEY_ID = "AKIA..."
$env:AWS_SECRET_ACCESS_KEY = "your-secret-key"
$env:AWS_REGION = "us-east-1"
# If using temporary credentials (e.g. assumed role):
# $env:AWS_SESSION_TOKEN = "your-session-token"

python db/load_vec_to_rds.py
```

Use an IAM user or role that has permission to call **Bedrock** (`bedrock:InvokeModel` on the Titan model).

---

### 3. AWS SSO (IAM Identity Center) – common on company laptops

If your company uses **AWS SSO** (single sign-on):

1. Log in once:
   ```powershell
   aws sso login --profile your-profile-name
   ```
   (Replace `your-profile-name` with the profile in `%USERPROFILE%\.aws\config`.)

2. Tell the script to use that profile:
   ```powershell
   $env:AWS_PROFILE = "your-profile-name"
   $env:AWS_REGION = "us-east-1"
   python db/load_vec_to_rds.py
   ```

3. If the SSO session expires, run `aws sso login --profile your-profile-name` again and re-run the script.

---

### 4. Check that credentials work

From PowerShell:

```powershell
aws sts get-caller-identity
```

If this prints your account and user/role, credentials are valid. Then test Bedrock (same region as your script):

```powershell
aws bedrock-runtime invoke-model --model-id amazon.titan-embed-text-v1:0 --body "{\"inputText\":\"test\"}" --region us-east-1 output.json
```

If that succeeds, `load_vec_to_rds.py` should work once the same credentials are used (via CLI config, env vars, or AWS_PROFILE).

---

## Summary

| Cause | What to do |
|-------|------------|
| No credentials at all | Run `aws configure` or set `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` (and optionally `AWS_SESSION_TOKEN`). |
| Using SSO | Run `aws sso login --profile <name>`, then set `AWS_PROFILE=<name>` and `AWS_REGION=us-east-1` before the script. |
| Wrong region | Set `AWS_REGION=us-east-1` (or the region where Bedrock is enabled). |
| No Bedrock permission | Ask your admin to allow `bedrock:InvokeModel` (e.g. for `amazon.titan-embed-text-v1:0`) for your user/role. |

After credentials are set, run:

```powershell
python db/load_vec_to_rds.py
```

(or with `--limit 100` first to test).
