# Git Setup & Commit Guide

Here is the step-by-step guide for setting up Git, ignoring unwanted files (e.g. Maven wrapper), committing, and pushing to a remote repository.

---

## 1. Add Files to `.gitignore`
To prevent `mvnw`, `mvnw.cmd`, and `.mvn/` from being tracked:
Add the following entries to `.gitignore`:
```gitignore
### Maven Wrapper ###
mvnw
mvnw.cmd
.mvn/
```

---

## 2. Unstage / Untrack Excluded Files (if already staged)
If unwanted files were already tracked or staged in Git, remove them from the index without deleting them locally:
```bash
git rm -r --cached mvnw mvnw.cmd .mvn
```

---

## 3. Stage All Project Files
Stage all valid project files:
```bash
git add -A
```

---

## 4. Commit Your Changes
Create the commit with a descriptive message:
```bash
git commit -m "Initial commit for order-event-service"
```

---

## 5. Set the Remote Origin URL (if not already added)
Link your local repository to GitHub:
```bash
git remote add origin https://github.com/CodingIsAnArtOfMind/order-event-service.git
```

---

## 6. Set Branch and Push to GitHub
Rename the branch to `main` (if needed) and push:
```bash
git branch -M main
git push -u origin main
```

---

## Summary Checklist
```bash
# 1. Initialize git (if needed)
git init

# 2. Add remote repository
git remote add origin https://github.com/CodingIsAnArtOfMind/order-event-service.git

# 3. Stage all files
git add -A

# 4. Unstage unwanted files if tracked
git rm -r --cached mvnw mvnw.cmd .mvn 2>/dev/null || true

# 5. Commit
git commit -m "Initial commit"

# 6. Push to remote
git branch -M main
git push -u origin main
```
