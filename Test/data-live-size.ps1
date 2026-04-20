# Run with .\data_live_size.ps1
# / 1KB → / 1MB → / 1GB

param(
    [string]$File = "data_live.txt"
)

while ($true) {
    Clear-Host
    if (Test-Path $File) {
        $size = (Get-Item $File).Length
        "{0}: {1:N2} KB" -f $File, ($size / 1KB)
    } else {
        "${File}: not found"
		break
    }
    Start-Sleep -Seconds 1
}