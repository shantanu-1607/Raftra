# Raftra CLI installer (Windows, PowerShell 5.1 or later).
#   irm https://raw.githubusercontent.com/shantanu-1607/Raftra/main/site/install.ps1 | iex
# From cmd.exe:
#   powershell -ExecutionPolicy Bypass -c "irm https://raw.githubusercontent.com/shantanu-1607/Raftra/main/site/install.ps1 | iex"
# Environment overrides:
#   RAFTRA_INSTALL_DIR     where to install (default: %LOCALAPPDATA%\Programs\raftra)
#   RAFTRA_DOWNLOAD_BASE   where to download from (default: the latest GitHub release)

# Everything runs inside a script block so a failure stops the script without
# closing the visitor's terminal (`exit` would, under `iex`).
& {
  $ErrorActionPreference = 'Stop'
  $ProgressPreference = 'SilentlyContinue' # the progress bar makes Invoke-WebRequest very slow on 5.1

  $repo = 'shantanu-1607/Raftra'
  $bin = 'raftra-cli.exe'
  $base = if ($env:RAFTRA_DOWNLOAD_BASE) { $env:RAFTRA_DOWNLOAD_BASE } else { "https://github.com/$repo/releases/latest/download" }
  $dir = if ($env:RAFTRA_INSTALL_DIR) { $env:RAFTRA_INSTALL_DIR } else { Join-Path $env:LOCALAPPDATA 'Programs\raftra' }
  $onWindows = $env:OS -eq 'Windows_NT'

  # The OS architecture, not this PowerShell's: x64 PowerShell on an ARM PC still wants the arm64 build.
  $cpu = $null
  try { $cpu = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString() } catch { }
  if (-not $cpu) { $cpu = if ($env:PROCESSOR_ARCHITEW6432) { $env:PROCESSOR_ARCHITEW6432 } else { $env:PROCESSOR_ARCHITECTURE } }
  switch ($cpu.ToUpper()) {
    { $_ -in 'X64', 'AMD64' } { $arch = 'amd64' }
    'ARM64' { $arch = 'arm64' }
    default { throw "Unsupported CPU architecture: $cpu. Raftra ships x64 and ARM64 builds." }
  }

  # Windows PowerShell 5.1 may default to TLS 1.0, which GitHub refuses.
  if ($PSVersionTable.PSEdition -ne 'Core') {
    [Net.ServicePointManager]::SecurityProtocol = [Net.ServicePointManager]::SecurityProtocol -bor [Net.SecurityProtocolType]::Tls12
  }

  $asset = "raftra-cli_windows_$arch.zip"
  $tmp = Join-Path ([IO.Path]::GetTempPath()) ("raftra-" + [guid]::NewGuid().ToString('N'))
  New-Item -ItemType Directory -Path $tmp | Out-Null
  try {
    Write-Host "Downloading $asset ..."
    $zip = Join-Path $tmp $asset
    $sums = Join-Path $tmp 'checksums.txt'
    Invoke-WebRequest -UseBasicParsing -Uri "$base/$asset" -OutFile $zip
    Invoke-WebRequest -UseBasicParsing -Uri "$base/checksums.txt" -OutFile $sums

    $line = Get-Content $sums | Where-Object { $_ -match ('\s' + [regex]::Escape($asset) + '$') } | Select-Object -First 1
    if (-not $line) { throw "No checksum listed for $asset" }
    $expected = ($line -split '\s+')[0].ToLower()
    $actual = (Get-FileHash -Algorithm SHA256 -Path $zip).Hash.ToLower()
    if ($expected -ne $actual) { throw "Checksum mismatch for ${asset}: refusing to install" }

    $out = Join-Path $tmp 'unpacked'
    Expand-Archive -Path $zip -DestinationPath $out -Force
    New-Item -ItemType Directory -Force -Path $dir | Out-Null
    Copy-Item -Path (Join-Path $out $bin) -Destination (Join-Path $dir $bin) -Force
  }
  finally {
    Remove-Item -Recurse -Force $tmp -ErrorAction SilentlyContinue
  }
  Write-Host "Installed $bin to $dir"

  if ($onWindows) {
    $userPath = [Environment]::GetEnvironmentVariable('Path', 'User')
    $parts = if ($userPath) { $userPath -split ';' | Where-Object { $_ } } else { @() }
    if ($parts -notcontains $dir) {
      [Environment]::SetEnvironmentVariable('Path', (($parts + $dir) -join ';'), 'User')
      Write-Host "Added $dir to your user PATH. Terminals opened from now on will find raftra-cli."
    }
    if (($env:Path -split ';') -notcontains $dir) { $env:Path = "$env:Path;$dir" }
  }
  Write-Host "Try it:  raftra-cli status"
}
