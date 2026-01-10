# dscc202-402-spring2026
Course materials for DSCC-202-402

## Access Databricks Workspace 
[Establish your Databricks Free Account](https://www.databricks.com/learn/free-edition))


Here is some helpful information about importing archives into the Databricks Environment: 
[Getting Started](https://docs.databricks.com/aws/en/getting-started/)

Import the DBC archive from the Learning Spark v2 GitHub repository into your account. (This is the code that goes along with the textbook)
[DBC Archive](https://github.com/databricks/LearningSparkV2/blob/master/notebooks/LearningSparkv2.dbc)

## Establishing a GitHub account if you do not have one already
[Sign up for a new github account](https://docs.github.com/en/github/getting-started-with-github/signing-up-for-a-new-github-account) <br>

## Fork the class repository into your account
Could you unlock the dsc402 repository into your new account?  Note: this will create a copy of the course repo for you to add and work on within your
own account.<br>
Go to https://github.com/lpalum/dscc202-402-spring2026 and hit the fork button while you are logged into your GitHub account.

## Connect Your Forked Repository to Databricks Using Git Folders

Instead of cloning to your local machine, you'll import your forked repository directly into your Databricks workspace using Git Folders.

### Step 1: Link Your GitHub Account to Databricks

1. In your Databricks workspace, click your **username** (top right) → **Settings**
2. Go to **Linked accounts** tab → **Add Git credential**
3. Select **GitHub** → **Link Git account**
4. On the GitHub authorization page, click **Authorize Databricks**
5. Install the Databricks GitHub App and select your repositories

**Note**: You only need to do this once.

### Step 2: Clone Your Repository into Databricks

1. In the Databricks sidebar, click **Workspace**
2. Navigate to your user folder
3. Click **Create** → **Git folder**
4. Provide:
   - **Git repository URL**: `https://github.com/your-github-username/dscc202-402-spring2026`
   - **Git provider**: GitHub
   - **Git folder name**: `dscc202-402-spring2026`
5. Click **Create Git folder**

### Step 3: Verify Your Setup

1. Expand the Git folder in your workspace
2. Navigate to `labs/` and open `0.1 - Spark Core.py`
3. Verify the notebook opens and shows the Git branch indicator at the top

**Troubleshooting**:
- Verify your GitHub username is correct in the URL
- Ensure you completed Step 1 (linking GitHub account)
- Check that your forked repository is public or Databricks has access

### Working with Git Folders

- **Make changes**: Edit notebooks directly in Databricks
- **Commit & push**: Use the Git menu (top right of notebook) to commit and push to GitHub
- **Pull updates**: Use the Git menu to pull changes from your fork


