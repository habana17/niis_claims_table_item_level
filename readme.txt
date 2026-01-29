# Adding a New Git Repository

## Initial Setup

1. Check the Git status:
   Make sure you are in your project folder:
   cd /path/to/your/folder
   git status

2. Initialize Git (if not already a Git repository):
   git init

3. Add all files:
   git add .

4. Commit changes:
   git commit -m "Initial commit"

5. Add the remote GitHub repository:
   git remote add origin https://github.com/habana17/niis_claims_table_item_level.git

6. Verify the remote:
   git remote -v

7. Push to GitHub:
   git push -u origin master
   # Note: If your default branch is 'main', use:
   # git push -u origin main

---

## Adding New Files (e.g., a new PSQL script)

1. Check the status to see untracked files:
   git status

2. Stage the new file(s):
   git add your_new_script.sql
   # or add all changes: git add .

3. Commit the changes:
   git commit -m "Add new PSQL script: your_new_script.sql"

4. Push to GitHub:
   git push

---

**Notes:**
- Always make sure you are inside your project folder before running commands.
- You can stage files individually or all at once depending on your workflow.
- Replace `master` with `main` if your repository uses `main` as the default branch.
