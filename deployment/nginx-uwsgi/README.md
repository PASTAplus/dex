# systemd + Nginx + uWSGI deployment

Example configuration files for Nginx + uWSGI + systemd deployment.

Copy the files to their standard locations:

```
/etc/nginx/sites-enabled/dex.conf -> ../sites-available/dex.conf
/etc/systemd/system/dex.service
/etc/uwsgi/dex.ini
```

Then edit them as required.

# Troubleshooting

- Error: `dbm.error: db type is dbm.gnu, but the module is not available`
- Fix: Delete `perf.db`


