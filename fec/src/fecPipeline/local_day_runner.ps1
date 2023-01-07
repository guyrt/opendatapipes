# local runner to cheaply iterate over days 1 at at time

$run_date = 20220108

# hangs up if you try to save output.
# az ml job create --file .\fecUnzipToMergePipeline.yaml --resource-group rg-fecdata --workspace fecaml --set inputs.run_date=$run_date

# $run_obj = cat /tmp/az.json | ConvertFrom-Json
# $run_name = $run_obj.name

$run_name = "shy_calypso_4cvyh1g8yv"

echo "Running $run_date named $run_name";

function get_status($name) {
    $az_status_obj = az ml job show --name $run_name --resource-group rg-fecdata --workspace fecaml | ConvertFrom-Json

    $az_status = $az_status_obj.status
    return $az_status
}

$az_status = get_status($run_name)

$last_date_check = Get-Date
while ($az_status -eq "Running") {
    $now = Get-Date
    if (($now - $last_date_check).TotalMinutes -gt 1) {
        echo "$az_status at $now"
        $last_date_check = Get-Date
    }
    Start-Sleep -s 30
    $az_status_obj = az ml job show --name $run_name --resource-group rg-fecdata --workspace fecaml | ConvertFrom-Json
    $az_status = get_status($run_name)
}

echo $az_status