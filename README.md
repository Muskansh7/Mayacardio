# MayaCardio case: fault-tolerant biosignal pipeline

Python 3.9+ standard library only (no pip installs). The device and cloud share one processing module (`pipeline/processing.py`).

## Layout
- `pipeline/`: device program: collector (mock to SQLite), uploader (SQLite to cloud), local processor, CLI
- `cloud/server.py`: cloud ingest, storage and online processing (stdlib HTTP plus SQLite)
- `standin_mock/`: a **stand-in** mock and ground-truth checker. The official download link was blank in the brief, so these were written from section 6 for testing only. Judging uses the official mock.
- `tests/`: reference implementation, processing equivalence test, failure-test orchestrator, network-cut proxy
- `deploy/`: systemd units
- `evidence/`: test outputs

## Install on a fresh Raspberry Pi 4 (Raspberry Pi OS 64-bit)
```
sudo apt install -y python3 git
git clone <repo> /opt/pipeline && cd /opt/pipeline
sudo mkdir -p /etc/pipeline /var/lib/pipeline
sudo cp deploy/config.example.json /etc/pipeline/config.json   # set cloud_url and token
sudo cp deploy/pipeline.service /etc/systemd/system/ && sudo systemctl enable --now pipeline
sudo ln -s /opt/pipeline/bin/pipeline /usr/local/bin/pipeline
```

## Cloud (free tier: Oracle Cloud Always Free Ampere VM or GCP e2-micro)
```
PIPELINE_TOKEN=<secret> PIPELINE_PORT=8080 PIPELINE_CLOUD_DB=/var/lib/pipeline-cloud/cloud.db python3 cloud/server.py
```
Put Caddy (free) in front for HTTPS. `deploy/cloud.service` is a systemd unit for this.

## Commands
`pipeline status` · `pipeline export --session ID --from device|cloud` · `pipeline results --session ID` · `pipeline sessions` · `pipeline verify`

## Tests
```
python3 tests/test_processing.py          # criterion 7: incremental == naive reference
sudo python3 tests/failure_tests.py       # all failure scenarios (root for tmpfs/clock tests)
```
