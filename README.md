# Prerequisites
- git
- conda

# Setup
- Install local postgres, I used homebrew on my mac
- Configure user and database
  - Create a dedicated user for budget app with a password
  ```sql
  CREATE ROLE budgeter WITH LOGIN PASSWORD 'secure_password_here';
  CREATE DATABASE finance OWNER budgeter;
  ALTER ROLE budgeter SET timezone TO 'UTC';
  ```

# Usage
- Start the postgres db before you need to use it via 
```sql
pg_ctl -D /opt/homebrew/var/postgresql@18 start
```
- You can run sql commands by first running:
```sql
psql postgres
```
- Stop the postgres db when you're done with it

# Troubleshooting

If you experience the following error:
```python
  raise exceptions.RefreshError(
google.auth.exceptions.RefreshError: ('invalid_grant: Token has been expired or revoked.', {'error': 'invalid_grant', 'error_description': 'Token has been expired or revoked.'})`
```

Try deleting your `token.json` and then run the script again.

# Todo:

- [ ] Automate pulling CSV from Fidelity
- [ ] Automate renaming of column names in CSV 
- [ ] figure out how to reconcile old data when data arrives late (e.g. I already pulled data for 2 months ago, but then I fixed the credentials for a connected account and all of the historical transactions only popped up now, so if I only pulled in this month's data, I'd be losing data that is accessible to me by forgetting to pull it in and not having a way to deduplicate it from the existing data)
- [ ] automate backups of my local postgres db
- [ ] Use Fidelity's way of having the Hidden Transaction being an additional boolean column, and migrate the existing historical data where I had overridden the cateogry with "Hide from budgets and trends"
- [X] Automate the calculation of date start needed for next delta file
- [X] Automate removal of header line from file "Spending Transactions:"
- [X] Automate removal of trailing rows in CSV
- [X] Automate renaming file based on the dates