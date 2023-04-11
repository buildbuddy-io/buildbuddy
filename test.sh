missing_assets=$(./tools/check_release_assets_uploaded.py --release_version_tag=v2.12.30 2>&1)
stderr=$missing_assets | ?{ $_ -is [System.Management.Automation.ErrorRecord] }
stdout=$missing_assets | ?{ $_ -isnot [System.Management.Automation.ErrorRecord] }

echo "stderr $stderr"
echo "stdout $stdout"
