To start beamlime detector data and dashboard service
```
sudo systemctl start beamlime-detector@nmx.service
sudo systemctl start beamlime-dashboard@nmx.service
```

To view the logs:
```
sudo tail -f /var/log/beamlime-detector-nmx.log
sudo tail -f /var/log/beamlime-dashboard-nmx.log
```
