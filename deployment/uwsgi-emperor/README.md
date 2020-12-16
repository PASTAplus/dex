# systemd + Nginx + uWSGI Emperor + Vassals

Example configuration files for using the uWSGI Emperor and Vassals system, which enables dynamically launching new services by dropping `.ini` files into the `vassals` directory.

Copy the files to their standard locations:

```
/etc/systemd/system/dex.service
/etc/uwsgi/emperor.ini
/etc/uwsgi/vassals/dex.ini
```

Then edit them as required.
