# Troubleshooting

If you experience the following error:
```python
  raise exceptions.RefreshError(
google.auth.exceptions.RefreshError: ('invalid_grant: Token has been expired or revoked.', {'error': 'invalid_grant', 'error_description': 'Token has been expired or revoked.'})`
```

Try deleting your `token.json` and then run the script again.