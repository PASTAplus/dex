# Deployment

# Troubleshooting

- Error: `dbm.error: db type is dbm.gnu, but the module is not available`
- Fix: Delete `perf.db`

- Error: `invalid request block size: 8157 (max 4096)...skip`
- Increase `buffer-size` in `uwsgi.ini`.
