## Installing the Chart

To install the chart with the release name `my-release`:

1. **Add the repository:**
   ```bash
   helm repo add skyvault https://dynoinc.github.io/skyvault-rs
   helm repo update
   ```

2. **Install the chart:**
   ```bash
   helm install my-release skyvault/skyvault --version 0.1.0
   ```

