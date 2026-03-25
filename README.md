# Prerequisites
- git
- conda

# Troubleshooting

If you experience the following error:
```python
  raise exceptions.RefreshError(
google.auth.exceptions.RefreshError: ('invalid_grant: Token has been expired or revoked.', {'error': 'invalid_grant', 'error_description': 'Token has been expired or revoked.'})`
```

Try deleting your `token.json` and then run the script again.

# Todo:

- [ ] figure out how to reconcile old data when data arrives late (e.g. I already pulled data for 2 months ago, but then I fixed the credentials for a connected account and all of the historical transactions only popped up now, so if I only pulled in this month's data, I'd be losing data that is accessible to me by forgetting to pull it in and not having a way to deduplicate it from the existing data)
- [ ] automate backups of my local postgres db