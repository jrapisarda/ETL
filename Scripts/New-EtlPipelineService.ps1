# ---------- user-adjustable values ----------
$LinuxHost   = 'my-linux-host'          # DNS name or IP
$LinuxUser   = 'admin'                  # account that can sudo
$ServicePath = '/etc/systemd/system/etl-pipeline.service'
$UnitContent = @'
[Unit]
Description=ETL Pipeline
After=network.target

[Service]
Type=simple
User=etl
Group=etl
WorkingDirectory=/opt/etl
ExecStart=/usr/bin/python3 /opt/etl/run.py
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
'@
# --------------------------------------------

# 1. Build a temporary file on Windows
$tmp = [System.IO.Path]::GetTempFileName()
$UnitContent | Out-File -FilePath $tmp -Encoding ascii -Force

# 2. Copy the file to the Linux box
scp -i "$HOME\.ssh\id_rsa" $tmp "${LinuxUser}@${LinuxHost}:/tmp/etl-pipeline.service"

# 3. Move it into place and reload systemd
ssh -i "$HOME\.ssh\id_rsa" "${LinuxUser}@${LinuxHost}" @"
sudo mv /tmp/etl-pipeline.service $ServicePath
sudo chown root:root $ServicePath
sudo chmod 644 $ServicePath
sudo systemctl daemon-reload
sudo systemctl enable etl-pipeline.service
"@

# 4. Clean up
Remove-Item $tmp -Force