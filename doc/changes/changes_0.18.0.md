# 0.18.0 - 2021-02-14

- Enforced TCP keep-alive for HTTP transport connections for Linux, MacOS and Windows. Keep-alive is required to address [Google Cloud firewall rules](https://cloud.google.com/compute/docs/troubleshooting/general-tips#communicatewithinternet) dropping idle connections after 10 minutes.

